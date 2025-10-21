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
          dict: keys 'shm_map','size_map','version_map','refcount_map','lock','event_map'
        If shared_maps is None then local (single-process) dicts are used.
        """
        self.logger = logger or self._create_default_logger()
        # local lock for intra-process operations (fallback)
        self._local_lock = threading.RLock()

        # normalize maps
        if shared_maps is None:
            self._shm_map = {}
            self._size_map = {}
            self._version_map = {}
            self._refcount_map = {}
            self._cross_lock = None
            self._event_map = {}
        else:
            if isinstance(shared_maps, (tuple, list)):
                (self._shm_map, self._size_map,
                 self._version_map, self._refcount_map,
                 self._cross_lock, self._event_map) = shared_maps
            elif isinstance(shared_maps, dict):
                self._shm_map = shared_maps.get("shm_map")
                self._size_map = shared_maps.get("size_map")
                self._version_map = shared_maps.get("version_map")
                self._refcount_map = shared_maps.get("refcount_map")
                self._cross_lock = shared_maps.get("lock")
                self._event_map = shared_maps.get("event_map")
            else:
                raise TypeError("shared_maps must be tuple/list/dict or None")

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


# ---------------------- Node ----------------------
class Node:
    """
    Node uses Heap internally. Dot notation backed by shared memory pointers.
    """
    def __init__(self, max_fields=10, memory_limit=None, warn_threshold=0.8,
                 logger=None, shared_maps=None, data_refs=None):
        self.logger = logger or self._create_default_logger()
        self.heap = Heap(self.logger, shared_maps=shared_maps)
        self.max_fields = max_fields
        self.memory_limit = memory_limit
        self.warn_threshold = warn_threshold

        self.data = {}           # field_name -> uuid.UUID
        self.field_memory = {}
        self.children = {}
        self.id = uuid.uuid4()
        self.fields_count = 0

        self._memray_tracker = None
        self._memray_report_path = None

        # cache (in-process) for deserialized values + their digest timestamp
        super().__setattr__("_cache", {})
        super().__setattr__("_cache_version", {})

        # attach incoming refs (child process): inc_ref to claim
        if data_refs:
            for name, us in data_refs.items():
                u = uuid.UUID(us)
                self.data[name] = u
                try:
                    self.heap.inc_ref(u)
                except Exception:
                    pass

    @property
    def data_refs(self):
        return {k: str(v) for k, v in self.data.items()}

    def __getattr__(self, name):
        # provide dot-style access
        if name not in self.data:
            self.add_field(name, None)
            return None

        ptr = self.data[name]
        key = str(ptr)

        # read version + event to decide whether to use cache
        with self.heap._lock():
            ver = self.heap._version_map.get(key)
            ev = self.heap._event_map.get(key)

        # if write-in-progress wait a short time
        if ev:
            try:
                if hasattr(ev, "is_set"):
                    # wait until cleared
                    start = time.time()
                    while ev.is_set() and time.time() - start < 0.5:
                        time.sleep(0.01)
                else:
                    # boolean flag - busy wait
                    start = time.time()
                    while self.heap._event_map.get(key) and time.time() - start < 0.5:
                        time.sleep(0.01)
            except Exception:
                pass

        # if cached and version matches, return cache
        cache = super().__getattribute__("_cache")
        cache_ver = super().__getattribute__("_cache_version")
        if name in cache and cache_ver.get(name) == ver:
            return cache[name]

        # else read from heap and update cache
        val = self.heap.read(ptr)
        cache[name] = val
        cache_ver[name] = ver
        return val

    def __setattr__(self, name, value):
        reserved = {
            'max_fields', 'memory_limit', 'warn_threshold', 'data', 'field_memory',
            'children', 'id', 'memory_usage', 'total_memory_usage', 'logger',
            '_memray_tracker', '_memray_report_path', 'heap', '_cache', '_cache_digest',
            'fields_count', '_write_lock', '_version', '_gc_thread', '_stop_gc'
        }
        if name in reserved or name.startswith('_'):
            super().__setattr__(name, value)
            return
        # update or create
        if name in self.data:
            ptr = self.data[name]
            # write with transactional semantics
            self.heap.write(ptr, value)
            # update cache version (read new version)
            with self.heap._lock():
                ver = self.heap._version_map.get(str(ptr))
            self._cache[name] = value
            self._cache_version[name] = ver
            self.field_memory[name] = self._get_object_size(value)
        else:
            ptr = self.heap.alloc(value)
            self.data[name] = ptr
            self.field_memory[name] = self._get_object_size(value)
            self.fields_count = len(self.data)

    def remove_field(self, name):
        if name not in self.data:
            return
        ptr = self.data.pop(name)
        self.field_memory.pop(name, None)
        self.fields_count = len(self.data)
        try:
            # decrement refcount so GC can free if 0
            self.heap.dec_ref(ptr)
        except Exception:
            try:
                self.heap.free(ptr)
            except Exception:
                pass

    def _create_default_logger(self):
        logger = logging.getLogger(f"Node-{id(self)}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter("%(asctime)s | %(levelname)-6s | %(name)s | %(message)s", "%H:%M:%S")
            handler.setFormatter(fmt)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def _get_object_size(self, obj):
        # approximate
        size = sys.getsizeof(obj)
        if isinstance(obj, (list, dict, set, tuple)):
            for i in obj:
                size += self._get_object_size(i)
        elif isinstance(obj, str):
            size += len(obj.encode('utf-8'))
        elif isinstance(obj, Node):
            size += obj.get_total_memory_usage()
        return size

    def get_field_value(self, field_name):
        return getattr(self, field_name)

    def set_field_value(self, field_name, value):
        setattr(self, field_name, value)

    # memray helpers kept
    def start_memray_tracking(self, path="memray_trace.bin"):
        if not memray:
            self.logger.warning("memray not installed")
            return
        self._memray_report_path = path
        self._memray_tracker = memray.Tracker(path)
        self._memray_tracker.__enter__()

    def stop_memray_tracking(self):
        if not self._memray_tracker:
            return
        self._memray_tracker.__exit__(None, None, None)
        self._memray_tracker = None

    def print_memray_report(self, html_path=None):
        if not self._memray_report_path or not os.path.exists(self._memray_report_path):
            self.logger.warning("No memray report file")
            return
        html_path = html_path or (self._memray_report_path + ".html")
        os.system(f"memray flamegraph {self._memray_report_path} -o {html_path}")

    def get_memory_report(self, deep=False):
        report = {
            "node_id": str(self.id),
            "fields": {k: {"size_bytes": v, "ptr": str(self.data[k])} for k, v in self.field_memory.items()},
            "total": f"{self.total_memory_usage} bytes"
        }
        if deep and self.children:
            report["children"] = {k: v.get_memory_report(deep=True) for k, v in self.children.items()}
        return report

    def get_total_memory_usage(self):
        self.update_memory_usage()
        return self.total_memory_usage

    def update_memory_usage(self):
        self.memory_usage = sum(self.field_memory.values())
        self.total_memory_usage = self.memory_usage + sum(c.get_total_memory_usage() for c in self.children.values())
        if self.memory_limit and self.total_memory_usage > self.memory_limit * self.warn_threshold:
            self.logger.warning("[Node] memory high")


if __name__ == "__main__":

    def child_proc(shared_maps, data_refs):
        node = Node(shared_maps=shared_maps, data_refs=data_refs)
        print("[child] before:", node.field_str)
        node.field_str = "Modified by child"
        print("[child] after:", node.field_str)
        # release local claim
        node.remove_field("field_str")

    mgr = Manager()
    shm_map = mgr.dict()
    size_map = mgr.dict()
    version_map = mgr.dict()
    refcount_map = mgr.dict()
    lock = mgr.Lock()
    event_map = mgr.dict()   # will hold manager.Event proxies or booleans

    shared_maps = (shm_map, size_map, version_map, refcount_map, lock, event_map)

    node = Node(shared_maps=shared_maps)
    node.field_str = "Hello from parent"
    print("[parent] before child:", node.field_str)

    # prepare data_refs and launch child
    refs = node.data_refs
    p = Process(target=child_proc, args=(shared_maps, refs))
    p.start()
    p.join()

    # small delay to let manager sync
    time.sleep(0.1)
    print("[parent] after child:", node.field_str)
    print(node.get_memory_report())

    # cleanup all
    node.remove_field("field_str")
    node.heap._cleanup_all()

    
