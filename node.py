import uuid
import sys
import logging
import os
import pickle
from multiprocessing import shared_memory, Manager, Process, RLock
import atexit

try:
    import memray
except ImportError:
    memray = None



########################################################################################################################
# XXX: _Heap Class                                                                                                     #
########################################################################################################################
class _Heap:
    """ Memory manager that holds unique UUID-pointers to objects in shared memory. Can be accessed by other Python
    threads and processes. Uses Pickle to serialize objects before pushing them to shared memory as only byte-like
    structures can be read during IPC"""

    SLAB_SIZES = [64, 128, 256, 512, 1024, 2048, 4096, 8192]

    def __init__(self, logger=None, shared_maps=None):
        self.lock = RLock()
        # NOTE: Free slabs
        self._free_blocks = {size: [] for size in self.SLAB_SIZES}
        # NOTE: Shared dicts (for multiprocess interactions)
        if shared_maps:
            self._shm_map, self._size_map = shared_maps
        else:
            self._shm_map, self._size_map = {}, {}
        self.logger = logger or self._create_default_logger()
        # NOTE: Register cleanup at exit
        atexit.register(self._cleanup_all)

    def _choose_slab_size(self, size):
        """ Round up to the nearest slab size."""
        for slab in self.SLAB_SIZES:
            if size <= slab:
                return slab
        # NOTE: Large objects get exact size
        return size

    def _create_default_logger(self):
        logger = logging.getLogger("_Heap")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", "%H:%M:%S")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def _serialize(self, obj):
        return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

    def _deserialize(self, buf):
        return pickle.loads(buf)

    def alloc(self, obj):
        data = self._serialize(obj)
        size_needed = len(data)
        slab_size = self._choose_slab_size(size_needed)

        with self.lock:
            if slab_size in self._free_blocks and self._free_blocks[slab_size]:
                shm = self._free_blocks[slab_size].pop()
            else:
                shm = shared_memory.SharedMemory(create=True, size=slab_size)

        shm.buf[:size_needed] = data
        u = uuid.uuid4()
        with self.lock:
            self._shm_map[str(u)] = (shm.name, slab_size)
            self._size_map[str(u)] = size_needed

        shm.close()
        self.logger.debug(f"[HEAP] Allocated {u} ({size_needed} bytes in slab {slab_size})")
        return u

    def read(self, ptr):
        """ Gets an object by UUID from shared memory."""
        key = str(ptr)
        with self.lock:
            shm_name = self._shm_map.get(key)
            size = self._size_map.get(key)
            if shm_name is None:
                raise KeyError(f"Invalid memory pointer: {ptr}")
        shm = shared_memory.SharedMemory(name=shm_name)
        buf = bytes(shm.buf[:size])
        obj = self._deserialize(buf)
        shm.close()
        return obj

    def write(self, ptr, value):
        key = str(ptr)
        with self.lock:
            shm_name, slab_size = self._shm_map[key]
        data = self._serialize(value)
        size_needed = len(data)

        if size_needed <= slab_size:
            # reuse existing block
            shm = shared_memory.SharedMemory(name=shm_name)
        else:
            # return old block to free list if it fits
            with self.lock:
                for s in self.SLAB_SIZES:
                    if s >= slab_size:
                        self._free_blocks[s].append(shared_memory.SharedMemory(name=shm_name, create=False))
                        break
            # allocate new slab
            slab_size = self._choose_slab_size(size_needed)
            shm = shared_memory.SharedMemory(create=True, size=slab_size)

        shm.buf[:size_needed] = data
        shm.close()
        with self.lock:
            self._shm_map[key] = (shm.name, slab_size)
            self._size_map[key] = size_needed
        self.logger.debug(f"[HEAP] Updated {ptr} ({size_needed} bytes in slab {slab_size})")

    def free(self, ptr):
        key = str(ptr)
        with self.lock:
            info = self._shm_map.pop(key, None)
            self._size_map.pop(key, None)
        if info:
            shm_name, slab_size = info
            # instead of unlink, add to free blocks
            with self.lock:
                self._free_blocks[slab_size].append(shared_memory.SharedMemory(name=shm_name, create=False))
            self.logger.debug(f"[HEAP] Freed {ptr} (added to slab pool {slab_size})")

    def cleanup(self, active_pointers):
        """ Ramoves all entities not listed in active_pointers."""
        for key in list(self._shm_map.keys()):
            if key not in map(str, active_pointers):
                self.free(uuid.UUID(key))

    def used(self):
        """ List of all active UUIDs."""
        with self.lock:
            return list(self._shm_map.keys())

    def _cleanup_all(self):
        """Cleanup all allocated and free shared memory blocks."""
        with self.lock:
            # Cleanup active blocks
            for key, info in list(self._shm_map.items()):
                try:
                    shm_name, slab_size = info
                    shm = shared_memory.SharedMemory(name=shm_name)
                    shm.close()
                    shm.unlink()
                    self.logger.debug(f"[HEAP] Auto-freed shared memory {key}")
                except FileNotFoundError:
                    pass
                except Exception as e:
                    self.logger.error(f"[HEAP] Failed to cleanup {key}: {e}")
            self._shm_map.clear()
            self._size_map.clear()

            # Cleanup free slabs (prevent leaks from pool)
            for slab_size, shm_list in self._free_blocks.items():
                for shm in shm_list:
                    try:
                        shm.close()
                        shm.unlink()
                        self.logger.debug(f"[HEAP] Freed pooled slab of size {slab_size}")
                    except FileNotFoundError:
                        pass
                    except Exception as e:
                        self.logger.error(f"[HEAP] Failed to cleanup free slab: {e}")
                self._free_blocks[slab_size] = []
        self.logger.info("[HEAP] Cleaned up all shared memory including pooled slabs")

    # Optional: context manager for explicit cleanup
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cleanup_all()

    # Optional: destructor as last-resort safety
    def __del__(self):
        try:
            self._cleanup_all()
        except Exception:
            # Ignore errors during interpreter shutdown
            pass

########################################################################################################################
# XXX: Node Class                                                                                                      #
########################################################################################################################
class Node:
    def __init__(self,
                 max_fields=10,
                 memory_limit=None,
                 warn_threshold=0.8,
                 logger=None,
                 shared_maps=None,
                 data_refs=None):
        self.logger = logger or self._create_default_logger()
        self.heap = _Heap(shared_maps=shared_maps)
        self.max_fields = max_fields
        self.memory_limit = memory_limit
        self.warn_threshold = warn_threshold
        self.data = {}              # field_name -> ptr (UUID)
        self.field_memory = {}      # field_name -> size
        self.children = {}          # child_name -> Node
        self.id = uuid.uuid4()
        self.memory_usage = 0
        self.total_memory_usage = 0
        self._memray_tracker = None
        self._memray_report_path = None
        self.logger.info(f"[INIT] Node {self.id} created with heap memory model")
        self.update_memory_usage()
        if data_refs:
            self.data = {k: uuid.UUID(v) for k, v in data_refs.items()}
        # LRU cache for lazy fields loading, initialize cache safely
        if '_cache' not in self.__dict__:
            super().__setattr__('_cache', {})  # dict: field_name -> value
        if '_cache_digest' not in self.__dict__:
            super().__setattr__('_cache_digest', {})  # dict: field_name -> last memory hash

    @property
    def data_refs(self):
        return {k: str(v) for k, v in self.data.items()}

    def __getattr__(self, name):
        if name not in self.data:
            self.add_field(name, None)

        ptr = self.data[name]
        cache = super().__getattribute__('_cache')
        cache_digest = super().__getattribute__('_cache_digest')

        # Unpack tuple: (shm_name, slab_size)
        shm_name, slab_size = self.heap._shm_map[str(ptr)]
        size = self.heap._size_map[str(ptr)]

        shm = shared_memory.SharedMemory(name=shm_name)
        buf = bytes(shm.buf[:size])
        shm.close()

        digest = hash(buf)

        if name in cache and cache_digest.get(name) == digest:
            return cache[name]

        value = self.heap._deserialize(buf)
        cache[name] = value
        cache_digest[name] = digest
        return value

    def __setattr__(self, name, value):
        reserved = {
            'max_fields', 'memory_limit', 'warn_threshold', 'data', 'field_memory', 'children', 'id', 'memory_usage',
            'total_memory_usage', 'logger', '_memray_tracker', '_memray_report_path', 'heap', '_cache', '_cache_digest',
        }
        if name in reserved:
            super().__setattr__(name, value)
        else:
            self.add_field(name, value)

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

    def add_field(self, field_name, value):
        if field_name in self.data:
            return self.set_field_value(field_name, value)
        ptr = self.heap.alloc(value)
        self.data[field_name] = ptr
        self.field_memory[field_name] = self._get_object_size(value)
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

    def get_memory_report(self, deep=False):
        report = {
            "node_id": str(self.id),
            "fields": {
                k: {
                    "size_bytes": v,
                    "ptr": str(self.data[k]),
                } for k, v in self.field_memory.items()
            },
            "total": f"{self.total_memory_usage} bytes"
        }
        if deep and self.children:
            report["children"] = {k: v.get_memory_report(deep=True) for k, v in self.children.items()}
        return report

    ####################################################################################################################
    # XXX:  Memray management                                                                                          #
    ####################################################################################################################
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # NOTE: Cleanup shared memory
        self.heap._cleanup_all()


if __name__ == "__main__":

    import time

    ####################################################################################################################
    # XXX:  Examples                                                                                                   #
    ####################################################################################################################
    def child(shared_maps, data_refs):
        node = Node(shared_maps=shared_maps, data_refs=data_refs)
        print("[Child] Before:", node.field_str)
        node.field_str = "Modified by child!"
        print("[Child] After:", node.field_str)
        time.sleep(1)  # allow Manager to sync
        node.heap.logger.info("[Child] Done updating shared memory")

    if __name__ == "__main__":
        manager = Manager()
        shared_maps = (manager.dict(), manager.dict())  # shared proxy dicts
        with Node(shared_maps=shared_maps) as node:
            node.field_str = "Hello, parent!"
            print("[Parent] Before:", node.field_str)

            p = Process(target=child, args=(shared_maps, node.data_refs))
            p.start()
            p.join()

            # Give a moment for manager sync before reading
            time.sleep(0.5)
            print("[Parent] After child:", node.field_str)
