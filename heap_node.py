#!/usr/bin/env python3
"""
Enhanced shared-memory Heap + Node with:
 - transactional versioning (timestamp)
 - per-pointer Events for write-in-progress
 - reference counting and background GC
 - manager-backed shared maps and Lock/Event so child processes can attach

Usage: run this file. The example at bottom demonstrates parent/child interaction.
"""
import uuid
import sys
import logging
import os
import pickle
import threading
import time
import atexit
from multiprocessing import shared_memory, Manager, Process

try:
    import memray
except ImportError:
    memray = None


class Heap:
    """
    Shared memory manager with transaction/versioning, per-pointer Event for write sync,
    refcounting and GC. Accepts manager-backed maps to cooperate across processes.
    """

    GC_INTERVAL = 1.0     # seconds between GC scans
    GC_GRACE = 2.0        # seconds to wait after refcount->0 before unlinking

    def __init__(self, logger=None, shared_maps=None):
        """
        shared_maps expected as a dict-like or tuple:
          tuple: (shm_map, size_map, version_map, refcount_map, lock, event_map)
          or legacy 2-tuple: (shm_map, size_map)
          If shared_maps is None then manager-backed maps are created (so child processes can attach).
        """
        self.logger = logger or self._create_default_logger()

        # local lock for intra-process operations (fallback)
        self._local_lock = threading.RLock()
        # optional cross-process lock (initialized to None or manager.RLock if manager available)
        self._cross_lock = None

        # Normalize shared maps and ensure all maps the Heap uses exist.
        # We prefer manager-backed dicts to allow child processes to inspect maps.
        if shared_maps:
            # support both legacy 2-tuple and new 6-tuple formats
            if len(shared_maps) == 2:
                # legacy: only shm_map & size_map given; create the rest using a Manager
                self._shm_map, self._size_map = shared_maps
                manager = Manager()
                self._version_map = manager.dict()
                self._refcount_map = manager.dict()
                self._event_map = manager.dict()
                self._free_blocks = manager.dict()
                # provide cross-process lock via manager
                try:
                    self._cross_lock = manager.RLock()
                except Exception:
                    self._cross_lock = None
            elif len(shared_maps) == 6:
                (self._shm_map,
                 self._size_map,
                 self._version_map,
                 self._refcount_map,
                 self._event_map,
                 self._free_blocks) = shared_maps
                # if a Manager is present in these maps, try to get a cross-process lock via Manager
                try:
                    # best-effort: create a manager RLock if Manager is present
                    manager = Manager()
                    self._cross_lock = manager.RLock()
                except Exception:
                    self._cross_lock = None
            else:
                raise ValueError("shared_maps must have 2 or 6 elements")
        else:
            # No shared_maps provided: create manager-backed maps for multi-process friendliness
            manager = Manager()
            self._shm_map = manager.dict()
            self._size_map = manager.dict()
            self._version_map = manager.dict()
            self._refcount_map = manager.dict()
            self._event_map = manager.dict()
            self._free_blocks = manager.dict()
            try:
                self._cross_lock = manager.RLock()
            except Exception:
                self._cross_lock = None

        self.logger.info("[Heap] initialized")

        # GC thread
        self._gc_stop = threading.Event()
        self._gc_thread = threading.Thread(target=self._gc_loop, name="Heap-GC", daemon=True)
        self._gc_thread.start()

        atexit.register(self._cleanup_all)

    def _create_default_logger(self):
        logger = logging.getLogger("Heap")
        if not logger.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter("%(asctime)s | %(levelname)-6s | %(name)s | %(message)s", "%H:%M:%S")
            handler.setFormatter(fmt)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def _lock(self):
        # return a context manager for cross-process lock if provided, else local lock
        return (self._cross_lock if self._cross_lock is not None else self._local_lock)

    def _serialize(self, obj):
        return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

    def _deserialize(self, buf):
        return pickle.loads(buf)

    # --- API ---
    def alloc(self, obj):
        """Allocate a new shared memory block for obj. Return uuid.UUID."""
        data = self._serialize(obj)
        size = len(data)
        shm = shared_memory.SharedMemory(create=True, size=size)
        shm.buf[:size] = data
        key = str(uuid.uuid4())
        now = time.time()
        with self._lock():
            self._shm_map[key] = shm.name
            self._size_map[key] = size
            self._version_map[key] = now
            self._refcount_map[key] = 1
            # create per-key event if cross-process event_map exists
            if isinstance(self._event_map, dict):
                # local dict use
                if key not in self._event_map:
                    self._event_map[key] = False
            else:
                # manager.dict or similar - set default flag
                self._event_map.setdefault(key, False)
        shm.close()
        self.logger.debug(f"[alloc] {key} size={size}")
        return uuid.UUID(key)

    def read(self, ptr, wait_for_write=False, timeout=1.0):
        """Read object stored at ptr. If wait_for_write True, wait until write-in-progress clears."""
        key = str(ptr)
        # optionally wait on event
        if wait_for_write:
            ev = self._event_map.get(key)
            if ev:
                # ev is a proxy Event? It might be boolean in local dict.
                try:
                    # Manager Event has is_set/wait/clear
                    if hasattr(ev, "is_set"):
                        # wait for clear (i.e. not set)
                        start = time.time()
                        while ev.is_set():
                            if time.time() - start > timeout:
                                break
                            time.sleep(0.01)
                    else:
                        # boolean flag - busy wait
                        start = time.time()
                        while ev and (time.time() - start) < timeout:
                            # refresh from map
                            ev = self._event_map.get(key)
                            time.sleep(0.01)
                except Exception:
                    pass

        with self._lock():
            shm_name = self._shm_map.get(key)
            used = self._size_map.get(key)
            if shm_name is None:
                raise KeyError("Invalid pointer: " + key)
        shm = shared_memory.SharedMemory(name=shm_name)
        buf = bytes(shm.buf[:used])
        obj = self._deserialize(buf)
        shm.close()
        return obj

    def write(self, ptr, value):
        """Write value to ptr in transactional manner: set write-event, write, update version, clear event."""
        key = str(ptr)
        data = self._serialize(value)
        size_needed = len(data)
        now = time.time()

        # get current info
        with self._lock():
            shm_name = self._shm_map.get(key)
            current_size = self._size_map.get(key)
            if shm_name is None:
                raise KeyError("Invalid pointer: " + key)
            # ensure an Event exists for this key (manager event or boolean slot)
            ev = self._event_map.get(key)
            if ev is None:
                # create a manager.Event if event_map is manager.dict
                try:
                    mgr_event = self._event_map.__class__  # just to force attribute access
                except Exception:
                    mgr_event = None
                # best-effort: set False placeholder
                self._event_map[key] = False

        # set event (manager.Event has set()/is_set())
        ev = self._event_map.get(key)
        try:
            if hasattr(ev, "set"):
                ev.set()
            else:
                self._event_map[key] = True
        except Exception:
            pass

        try:
            if size_needed <= current_size:
                shm = shared_memory.SharedMemory(name=shm_name)
                shm.buf[:size_needed] = data
                shm.close()
                with self._lock():
                    self._size_map[key] = size_needed
                    self._version_map[key] = now
            else:
                # allocate new block, swap, unlink old
                new_shm = shared_memory.SharedMemory(create=True, size=size_needed)
                new_shm.buf[:size_needed] = data
                new_name = new_shm.name
                new_shm.close()
                with self._lock():
                    old_name = self._shm_map.get(key)
                    self._shm_map[key] = new_name
                    self._size_map[key] = size_needed
                    self._version_map[key] = now
                # try unlink old
                try:
                    old = shared_memory.SharedMemory(name=old_name)
                    old.close()
                    old.unlink()
                except Exception:
                    pass
        finally:
            # clear write-in-progress event
            ev2 = self._event_map.get(key)
            try:
                if hasattr(ev2, "clear"):
                    ev2.clear()
                else:
                    self._event_map[key] = False
            except Exception:
                pass

        self.logger.debug(f"[write] {key} size={size_needed} ver={now}")

    # reference counting
    def inc_ref(self, ptr):
        key = str(ptr)
        with self._lock():
            self._refcount_map[key] = self._refcount_map.get(key, 0) + 1
            return self._refcount_map[key]

    def dec_ref(self, ptr):
        key = str(ptr)
        with self._lock():
            if key not in self._refcount_map:
                return 0
            self._refcount_map[key] = max(0, self._refcount_map[key] - 1)
            if self._refcount_map[key] == 0:
                # record time as version marker so GC can pick it up after grace
                self._version_map[key] = time.time()
            return self._refcount_map[key]

    def free(self, ptr):
        """Force free a pointer regardless of refcount."""
        key = str(ptr)
        with self._lock():
            shm_name = self._shm_map.pop(key, None)
            self._size_map.pop(key, None)
            self._version_map.pop(key, None)
            self._refcount_map.pop(key, None)
            ev = self._event_map.pop(key, None)
        if ev and hasattr(ev, "set"):
            try:
                ev.clear()
            except Exception:
                pass
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
        with self._lock():
            return list(self._shm_map.keys())

    # GC loop: unlink entries with refcount==0 older than GC_GRACE
    def _gc_loop(self):
        while not self._gc_stop.wait(self.GC_INTERVAL):
            try:
                now = time.time()
                to_free = []
                with self._lock():
                    for key, ref in list(self._refcount_map.items()):
                        if ref == 0:
                            ver = self._version_map.get(key, 0)
                            if now - ver >= self.GC_GRACE:
                                to_free.append(key)
                for key in to_free:
                    self.logger.debug(f"[GC] reclaiming {key}")
                    try:
                        self.free(uuid.UUID(key))
                    except Exception:
                        # try string key variant
                        try:
                            self.free(key)
                        except Exception as e:
                            self.logger.warning(f"[GC] failed to free {key}: {e}")
            except Exception as e:
                self.logger.exception(f"[GC] error: {e}")

    def _cleanup_all(self):
        # stop gc
        try:
            self._gc_stop.set()
            self._gc_thread.join(timeout=1.0)
        except Exception:
            pass
        # best-effort freeing everything
        with self._lock():
            keys = list(self._shm_map.keys())
        for k in keys:
            try:
                self.free(k)
            except Exception:
                pass
        self.logger.info("[cleanup_all] done")


class Node:
    def __init__(self,
                 max_fields=10,
                 memory_limit=None,
                 warn_threshold=0.8,
                 logger=None,
                 shared_maps=None,
                 data_refs=None):
        self.logger = logger or self._create_default_logger()
        # NOTE: keep your heap initialization (name may vary in your code)
        self.heap = Heap(self.logger, shared_maps=shared_maps)

        self.max_fields = max_fields
        self.memory_limit = memory_limit
        self.warn_threshold = warn_threshold

        # shared-data bookkeeping
        self.data = {}              # field_name -> ptr (UUID)
        self.field_memory = {}      # field_name -> size
        self.field_index = {}       # field_name -> index in fields_id
        self.children = {}          # child_name -> Node

        # identity / bookkeeping
        self.id = uuid.uuid4()
        self.fields_count = 0
        self.memory_usage = 0
        self.total_memory_usage = 0
        self.last_gc_check = 0

        # uuid buffer etc (unchanged)
        self._fields_id_buf = bytearray(self.max_fields * 16)
        self.fields_id = memoryview(self._fields_id_buf)

        # memray placeholders
        self._memray_tracker = None
        self._memray_report_path = None

        # caches for lazy loading
        if '_cache' not in self.__dict__:
            super().__setattr__('_cache', {})
        if '_cache_digest' not in self.__dict__:
            super().__setattr__('_cache_digest', {})

        self.logger.info(f"[INIT] Node {self.id} created with heap memory model")
        self.update_memory_usage()

        if data_refs:
            # attach pointers provided by parent process
            self.data = {k: uuid.UUID(v) for k, v in data_refs.items()}

    @property
    def data_refs(self):
        return {k: str(v) for k, v in self.data.items()}

    # centralized reserved-name test (prevents accidental shadowing)
    def _is_reserved_attr(self, name):
        # include internal attributes and all public methods that must not be shadowed
        reserved = {
            # internal bookkeeping
            'max_fields', 'memory_limit', 'warn_threshold', 'data', 'field_memory',
            'field_index', 'fields_count', 'children', 'id', 'memory_usage', 'total_memory_usage',
            'last_gc_check', 'logger', '_memray_tracker', '_memray_report_path',
            '_fields_id_buf', 'fields_id', 'heap', '_cache', '_cache_digest',

            # Node public methods that must not be shadowed by fields
            'add_field', 'set_field_value', 'get_field_value', 'remove_field',
            'get_memory_report', 'get_total_memory_usage',
            'start_memray_tracking', 'stop_memray_tracking', 'print_memray_report',
            'export_shared_refs', 'attach_shared_refs', 'cleanup_unused_memory',
            # dunder & others
            '__repr__', '__getattr__', '__setattr__', '__init__', 'data_refs',
        }
        if name.startswith('_'):
            return True
        return name in reserved

    # ---------- Dot notation read (with cache + change detection) ----------
    def __getattr__(self, name):
        # reserved/internal names should be handled by normal attribute lookup
        if self._is_reserved_attr(name):
            return super().__getattribute__(name)

        # ensure field exists
        if name not in self.data:
            self.add_field(name, None)

        ptr = self.data[name]

        # cache access
        cache = super().__getattribute__('_cache')
        cache_digest = super().__getattribute__('_cache_digest')

        # get backing shared memory info from heap mapping
        key = str(ptr)
        shm_info = self.heap._shm_map.get(key)
        if shm_info is None:
            # pointer present but heap mapping missing -> safe fallback
            val = self.heap.read(ptr)
            cache[name] = val
            cache_digest[name] = None
            return val

        # heap._shm_map may store either a name or (name, slab_size)
        if isinstance(shm_info, tuple):
            shm_name = shm_info[0]
        else:
            shm_name = shm_info

        size = self.heap._size_map.get(key)
        if size is None:
            # fallback: use heap.read
            val = self.heap.read(ptr)
            cache[name] = val
            cache_digest[name] = None
            return val

        # read raw bytes and compute digest to detect external modification
        shm = shared_memory.SharedMemory(name=shm_name)
        try:
            buf = bytes(shm.buf[:size])
        finally:
            shm.close()

        digest = hash(buf)

        if name in cache and cache_digest.get(name) == digest:
            return cache[name]

        # deserialize and update caches
        val = self.heap._deserialize(buf)
        cache[name] = val
        cache_digest[name] = digest
        return val

    # ---------- Dot notation write ----------
    def __setattr__(self, name, value):
        if self._is_reserved_attr(name):
            return super().__setattr__(name, value)

        # route through shared memory
        if name in self.data:
            self.set_field_value(name, value)
        else:
            self.add_field(name, value)

        # invalidate cache for that field
        if '_cache' in self.__dict__:
            self._cache.pop(name, None)
            self._cache_digest.pop(name, None)

    # ---------- core field operations ----------
    def add_field(self, field_name, value):
        if field_name in self.data:
            return self.set_field_value(field_name, value)
        ptr = self.heap.alloc(value)
        self.data[field_name] = ptr
        self.field_memory[field_name] = self._get_object_size(value)
        self.field_index[field_name] = len(self.field_index)
        self.fields_count = len(self.data)
        self.update_memory_usage()

    def set_field_value(self, field_name, value):
        if field_name not in self.data:
            return self.add_field(field_name, value)
        ptr = self.data[field_name]
        self.heap.write(ptr, value)
        self.field_memory[field_name] = self._get_object_size(value)
        self.update_memory_usage()

    def get_field_value(self, field_name):
        ptr = self.data.get(field_name)
        return self.heap.read(ptr) if ptr else None

    def remove_field(self, field_name):
        """Remove a field: free its shared memory and drop bookkeeping entries."""
        if field_name not in self.data:
            return
        ptr = self.data.pop(field_name)
        # free from heap (will unlink or add to pool based on your Heap implementation)
        try:
            self.heap.free(ptr)
        except Exception as e:
            # log but continue cleanup of local state
            self.logger.warning(f"[REMOVE] heap.free failed for {ptr}: {e}")
        # remove bookkeeping
        self.field_memory.pop(field_name, None)
        self.field_index.pop(field_name, None)
        # clear caches
        if '_cache' in self.__dict__:
            self._cache.pop(field_name, None)
            self._cache_digest.pop(field_name, None)
        self.fields_count = len(self.data)
        self.update_memory_usage()

    # ---------- remaining helpers / memray etc. (unchanged) ----------
    def _create_default_logger(self):
        logger = logging.getLogger(f"Node-{id(self)}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s", "%H:%M:%S")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def _get_object_size(self, obj):
        size = sys.getsizeof(obj)
        if isinstance(obj, (list, dict, set, tuple)):
            size += sum(self._get_object_size(i) for i in obj)
        elif isinstance(obj, str):
            size += len(obj.encode('utf-8'))
        elif isinstance(obj, Node):
            size += obj.get_total_memory_usage()
        return size

    def update_memory_usage(self):
        self.memory_usage = sum(self.field_memory.values())
        self.total_memory_usage = self.memory_usage + sum(c.get_total_memory_usage() for c in self.children.values())
        if self.memory_limit and self.total_memory_usage > self.memory_limit * self.warn_threshold:
            self.logger.warning(f"[WARN] Node {self.id} memory {self.total_memory_usage}/{self.memory_limit} bytes")

    def get_memory_report(self, deep=False):
        report = {
            "node_id": str(self.id),
            "fields": {
                k: {
                    "size_bytes": v,
                    "ptr": str(self.data[k]),
                    "index": self.field_index.get(k)
                } for k, v in self.field_memory.items()
            },
            "total": f"{self.total_memory_usage} bytes"
        }
        if deep and self.children:
            report["children"] = {k: v.get_memory_report(deep=True) for k, v in self.children.items()}
        return report

    def get_total_memory_usage(self):
        self.update_memory_usage()
        return self.total_memory_usage

    def start_memray_tracking(self, path="memray_trace.bin"):
        if not memray:
            self.logger.warning("Memray not installed")
            return
        self._memray_report_path = path
        self._memray_tracker = memray.Tracker(path)
        self._memray_tracker.__enter__()
        self.logger.info(f"[MEMRAY] Started tracking → {path}")

    def stop_memray_tracking(self):
        if not self._memray_tracker:
            return
        self._memray_tracker.__exit__(None, None, None)
        self.logger.info(f"[MEMRAY] Stopped tracking ({self._memray_report_path})")
        self._memray_tracker = None

    def print_memray_report(self, html_path=None):
        if not memray:
            self.logger.warning("Memray not installed")
            return
        if not self._memray_report_path or not os.path.exists(self._memray_report_path):
            self.logger.warning("No Memray report file found")
            return
        html_path = html_path or (self._memray_report_path + ".html")
        os.system(f"memray flamegraph {self._memray_report_path} -o {html_path}")
        self.logger.info(f"[MEMRAY] HTML report generated → {html_path}")

    def __repr__(self):
        return f"<Node id={self.id} fields={self.fields_count} mem={self.total_memory_usage}B>"



if __name__ == "__main__":

    manager = Manager()
    shared_maps = (manager.dict(), manager.dict())

    node = Node(shared_maps=shared_maps)
    node.field_str = "Hello, world!"
    print("[parent] before child:", node.field_str)

    def child_proc(shared_maps, data_refs):
        node_c = Node(shared_maps=shared_maps, data_refs=data_refs)
        print("[child] before:", node_c.field_str)
        node_c.field_str = "Modified by child"
        print("[child] after:", node_c.field_str)

    p = Process(target=child_proc, args=(shared_maps, node.data_refs))
    p.start()
    p.join()

    print("[parent] after child:", node.field_str)
    # remove in parent
    node.remove_field("field_str")
    print("removed. present?", "field_str" in node.data)

    
