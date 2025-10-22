#!/usr/bin/env python3

import uuid         # NOTE: used for unique pointers / IDs
import logging      # NOTE: logging for Heap/Node events
import os
import pickle       # NOTE: serialization for storing objects in shared memory
import threading    # NOTE: thread-safe GC
import time         # NOTE: timestamps for versioning / GC
import atexit       # NOTE: cleanup at process exit
from multiprocessing import shared_memory, Manager, Process



class Heap:
    """A process-safe shared memory manager with automatic garbage collection.

    The Heap class provides a high-level abstraction for allocating, storing, and managing Python objects in shared
    memory across multiple processes. Each allocated object is serialized (via `pickle`) and stored in a dedicated
    `multiprocessing.shared_memory.SharedMemory` block, identified by a unique UUID-based pointer. This design allows
    independent processes to share complex Python data structures without copying or using sockets.

    Internally, Heap maintains several cross-process dictionaries (managed by `multiprocessing.Manager`) that track:
      - shared memory names and sizes
      - object versions (timestamps)
      - reference counts
      - optional write-in-progress events
      - free block metadata

    These maps act as a lightweight shared-memory metadata registry. Reference counts are used to determine object
    lifetime. When a pointer’s reference count drops to zero, a background garbage collector (GC) thread reclaims its
    memory after a configurable grace period (`GC_GRACE`), ensuring that concurrent readers finish before freeing the
    block.

    The Heap can be run in “parent GC” mode, where only one instance performs garbage collection across processes,
    or in child mode, where instances rely on the parent to perform cleanup. All operations — allocation, read, write,
    reference management, and freeing — are synchronized using a thread- or process-level lock for data integrity.
    """

    GC_INTERVAL = 1.0  # NOTE: GC loop interval in seconds
    GC_GRACE = 2.0  # NOTE: grace period before freeing zero-ref memory

    def __init__(self, logger=None, shared_maps=None, manager=None, use_events=True, parent_gc=True):
        """
        Initialize a Heap instance.
        shared_maps: tuple of dicts for cross-process sharing (shm, size, version, refcount, event, free_blocks)
        manager: multiprocessing.Manager instance for cross-process dicts/events
        use_events: optional per-pointer Event for write-in-progress signaling
        parent_gc: if True, this instance runs GC; otherwise children rely on parent
        """
        self.logger = logger or self._create_default_logger()
        self._local_lock = threading.RLock()  # NOTE: local thread safety
        self._cross_lock = None  # HACK: optional cross-process lock
        self.manager = manager or Manager()  # NOTE: fallback to Manager if not provided
        self.use_events = use_events
        self.parent_gc = parent_gc

        # NOTE: Initialize shared maps (six components)
        if shared_maps:
            if len(shared_maps) == 6:
                (self._shm_map,       # NOTE: pointer -> shared_memory name
                 self._size_map,      # NOTE: pointer -> size in bytes
                 self._version_map,   # NOTE: pointer -> last updated timestamp
                 self._refcount_map,  # NOTE: pointer -> ref count
                 self._event_map,     # NOTE: pointer -> optional Event
                 self._free_blocks) = shared_maps
            else:
                raise ValueError("shared_maps must have 6 elements")
        else:
            # NOTE: create manager-backed dicts for cross-process safety
            self._shm_map = self.manager.dict()
            self._size_map = self.manager.dict()
            self._version_map = self.manager.dict()
            self._refcount_map = self.manager.dict()
            self._event_map = self.manager.dict()
            self._free_blocks = self.manager.dict()

        try:
            self._cross_lock = self.manager.RLock()  # NOTE: manager-backed RLock for cross-process safety
        except Exception:
            self._cross_lock = None  # HACK fallback if Manager.RLock fails

        self._gc_stop = threading.Event()  # NOTE: used to stop GC thread
        self._gc_thread = None
        if self.parent_gc:
            # NOTE: start GC thread if parent
            self._gc_thread = threading.Thread(target=self._gc_loop, name="Heap-GC", daemon=True)
            self._gc_thread.start()
            atexit.register(self._cleanup_all)  # TODO: ensure cross-process safety on exit

        self.logger.info("[Heap] initialized (parent GC=%s, events=%s)", self.parent_gc, self.use_events)

    def _create_default_logger(self):
        """Create a default logger if none provided"""
        logger = logging.getLogger("Heap")
        if not logger.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter("%(asctime)s | %(levelname)-6s | %(name)s | %(message)s", "%H:%M:%S")
            handler.setFormatter(fmt)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def _lock(self):
        """Return cross-process lock if available, else local thread lock"""
        return self._cross_lock if self._cross_lock else self._local_lock

    def _serialize(self, obj):
        """Serialize object to bytes for shared memory storage"""
        return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

    def _deserialize(self, buf):
        """Deserialize object from shared memory bytes"""
        return pickle.loads(buf)

    def alloc(self, obj):
        """Allocate object in shared memory and return pointer UUID"""
        data = self._serialize(obj)
        size = len(data)
        shm = shared_memory.SharedMemory(create=True, size=size)  # NOTE: create new shared memory block
        shm.buf[:size] = data  # NOTE: copy serialized bytes
        key = str(uuid.uuid4())  # NOTE: generate unique pointer ID
        now = time.time()
        with self._lock():
            # NOTE: Register allocation in shared maps
            self._shm_map[key] = shm.name
            self._size_map[key] = size
            self._version_map[key] = now
            self._refcount_map[key] = 1
            if self.use_events:
                self._event_map[key] = self.manager.Event()  # NOTE: per-pointer write event
        shm.close()
        self.logger.debug(f"[alloc] {key} size={size}")
        return uuid.UUID(key)

    def read(self, ptr, wait_for_write=False, timeout=1.0):
        """Read object from shared memory by pointer"""
        key = str(ptr)
        # NOTE: optional wait if write-in-progress
        if wait_for_write and self.use_events:
            event = self._event_map.get(key)
            if event:
                start = time.time()
                while event.is_set():
                    if time.time() - start > timeout:
                        break
                    time.sleep(0.01)
        with self._lock():
            shm_name = self._shm_map.get(key)
            size = self._size_map.get(key)
            if not shm_name or size is None:
                raise KeyError(f"Invalid pointer: {key}")
        shm = shared_memory.SharedMemory(name=shm_name)
        try:
            buf = bytes(shm.buf[:size])
            return self._deserialize(buf)
        finally:
            shm.close()

    def write(self, ptr, value):
        """Write object to shared memory by pointer, reallocate if size increases"""
        key = str(ptr)
        data = self._serialize(value)
        size_needed = len(data)
        now = time.time()
        with self._lock():
            shm_name = self._shm_map.get(key)
            current_size = self._size_map.get(key)
            if shm_name is None:
                raise KeyError(f"Invalid pointer: {key}")
            event = self._event_map.get(key) if self.use_events else None
            if event is None and self.use_events:
                event = self.manager.Event()
                self._event_map[key] = event
            if event:
                event.set()  # mark write-in-progress

        try:
            if size_needed <= current_size:
                # NOTE: overwrite in-place if current block is large enough
                shm = shared_memory.SharedMemory(name=shm_name)
                shm.buf[:size_needed] = data
                shm.close()
                with self._lock():
                    self._size_map[key] = size_needed
                    self._version_map[key] = now
            else:
                # NOTE: reallocate new shared memory if needed
                new_shm = shared_memory.SharedMemory(create=True, size=size_needed)
                new_shm.buf[:size_needed] = data
                new_name = new_shm.name
                new_shm.close()
                with self._lock():
                    old_name = self._shm_map.get(key)
                    self._shm_map[key] = new_name
                    self._size_map[key] = size_needed
                    self._version_map[key] = now
                try:
                    # NOTE: attempt to free old memory
                    old = shared_memory.SharedMemory(name=old_name)
                    old.close()
                    old.unlink()
                except Exception:
                    pass
        finally:
            if event:
                event.clear()  # NOTE: write complete

        self.logger.debug(f"[write] {key} size={size_needed} ver={now}")

    def inc_ref(self, ptr):
        """Increment reference count for pointer"""
        key = str(ptr)
        with self._lock():
            self._refcount_map[key] = self._refcount_map.get(key, 0) + 1
            return self._refcount_map[key]

    def dec_ref(self, ptr):
        """Decrement reference count; record timestamp if zero"""
        key = str(ptr)
        with self._lock():
            if key not in self._refcount_map:
                return 0
            self._refcount_map[key] = max(0, self._refcount_map[key] - 1)
            if self._refcount_map[key] == 0:
                self._version_map[key] = time.time()  # NOTE: mark zero-ref for GC
            return self._refcount_map[key]

    def free(self, ptr):
        """Free shared memory for pointer"""
        key = str(ptr)
        with self._lock():
            shm_name = self._shm_map.pop(key, None)
            self._size_map.pop(key, None)
            self._version_map.pop(key, None)
            self._refcount_map.pop(key, None)
            event = self._event_map.pop(key, None)
        if event:
            event.clear()
        if shm_name:
            try:
                shm = shared_memory.SharedMemory(name=shm_name)
                shm.close()
                shm.unlink()
                self.logger.debug(f"[free] {key} unlinked")
            except FileNotFoundError:
                pass
            except Exception as e:
                self.logger.error(f"[free] {key} unlink failed: {e}")

    def used(self):
        """Return list of currently allocated pointers"""
        with self._lock():
            return list(self._shm_map.keys())

    def _gc_loop(self):
        """Background GC thread: frees zero-refcount memory after GC_GRACE"""
        if not self.parent_gc:
            return
        while not self._gc_stop.wait(self.GC_INTERVAL):
            try:
                now = time.time()
                to_free = []
                with self._lock():
                    for key, ref in list(self._refcount_map.items()):
                        if ref == 0 and now - self._version_map.get(key, 0) >= self.GC_GRACE:
                            to_free.append(key)
                for key in to_free:
                    try:
                        self.free(key)
                        self.logger.debug(f"[GC] reclaimed {key}")
                    except Exception as e:
                        self.logger.warning(f"[GC] failed to free {key}: {e}")
            except Exception as e:
                self.logger.exception(f"[GC] error: {e}")

    def _cleanup_all(self):
        """Cleanup all memory on exit"""
        try:
            self._gc_stop.set()
            if self._gc_thread:
                self._gc_thread.join(timeout=1.0)
        except Exception:
            pass
        for key in list(self._shm_map.keys()):
            try:
                self.free(key)
            except Exception:
                pass
        self.logger.info("[cleanup_all] done")
