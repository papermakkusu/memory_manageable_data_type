import uuid
import sys
import types
import gc
import time
import logging
import os
import pickle
import threading
from multiprocessing import shared_memory, Manager, Process

try:
    import memray
except ImportError:
    memray = None


# ---------- Shared-memory-aware MemoryHeap ----------
class _MemoryHeap:
    """
    Менеджер памяти, хранящий объекты по UUID-указателям
    в сегментах настоящей shared memory, доступных из разных процессов.
    """
    def __init__(self, logger=None, shared_maps=None):
        self.lock = threading.Lock()

        # Shared dicts (for multi-process)
        if shared_maps:
            self._shm_map, self._size_map = shared_maps
        else:
            self._shm_map, self._size_map = {}, {}

        self.logger = logger or self._create_default_logger()

    def _create_default_logger(self):
        logger = logging.getLogger("MemoryHeap")
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
        """Сохраняет объект в shared memory и возвращает uuid-указатель."""
        u = uuid.uuid4()
        data = self._serialize(obj)
        size = len(data)
        shm = shared_memory.SharedMemory(create=True, size=size)
        shm.buf[:size] = data

        with self.lock:
            self._shm_map[str(u)] = shm.name
            self._size_map[str(u)] = size

        shm.close()
        self.logger.debug(f"[HEAP] Allocated {u} → {type(obj).__name__} ({size} bytes)")
        return u

    def read(self, ptr):
        """Получает объект по UUID из shared memory."""
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
        """Перезаписывает объект по UUID в shared memory."""
        key = str(ptr)
        with self.lock:
            shm_name = self._shm_map.get(key)
            if shm_name is None:
                raise KeyError(f"Invalid memory pointer: {ptr}")
        data = self._serialize(value)
        size = len(data)
        shm = shared_memory.SharedMemory(name=shm_name)
        if size > shm.size:
            shm.close()
            shm.unlink()
            shm = shared_memory.SharedMemory(create=True, size=size)
            with self.lock:
                self._shm_map[key] = shm.name
                self._size_map[key] = size
        shm.buf[:size] = data
        shm.close()
        self.logger.debug(f"[HEAP] Updated {ptr} → {type(value).__name__} ({size} bytes)")

    def free(self, ptr):
        """Освобождает сегмент shared memory."""
        key = str(ptr)
        with self.lock:
            shm_name = self._shm_map.pop(key, None)
            self._size_map.pop(key, None)
        if shm_name:
            shm = shared_memory.SharedMemory(name=shm_name)
            shm.close()
            shm.unlink()
            self.logger.debug(f"[HEAP] Freed memory at {ptr}")

    def cleanup(self, active_pointers):
        """Удаляет все объекты, которых нет в active_pointers."""
        for key in list(self._shm_map.keys()):
            if key not in map(str, active_pointers):
                self.free(uuid.UUID(key))

    def used(self):
        """Список всех активных UUID."""
        with self.lock:
            return list(self._shm_map.keys())


# ---------- Node ----------
class Node:
    def __init__(self, max_fields=10, memory_limit=None, warn_threshold=0.8, logger=None, shared_maps=None):
        self.logger = logger or self._create_default_logger()
        self.heap = _MemoryHeap(self.logger, shared_maps=shared_maps)

        self.max_fields = max_fields
        self.memory_limit = memory_limit
        self.warn_threshold = warn_threshold

        self.data = {}              # field_name -> ptr (UUID)
        self.field_memory = {}      # field_name -> size
        self.field_index = {}       # field_name -> index in fields_id
        self.children = {}          # child_name -> Node
        self.id = uuid.uuid4()
        self.fields_count = 0

        self.memory_usage = 0
        self.total_memory_usage = 0
        self.last_gc_check = 0

        self._fields_id_buf = bytearray(self.max_fields * 16)
        self.fields_id = memoryview(self._fields_id_buf)

        self._memray_tracker = None
        self._memray_report_path = None

        self.logger.info(f"[INIT] Node {self.id} created with heap memory model")
        self.update_memory_usage()

    # ---------- Dot notation ----------
    def __getattr__(self, name):
        if name not in self.data:
            self.add_field(name, None)
        return self.get_field_value(name)

    def __setattr__(self, name, value):
        reserved = {
            'max_fields', 'memory_limit', 'warn_threshold', 'data', 'field_memory',
            'field_index', 'fields_count', 'children', 'id', 'memory_usage', 'total_memory_usage',
            'last_gc_check', 'logger', '_memray_tracker', '_memray_report_path',
            '_fields_id_buf', 'fields_id', 'heap'
        }
        if name in reserved:
            super().__setattr__(name, value)
        else:
            self.add_field(name, value)

    # ---------- Логгер ----------
    def _create_default_logger(self):
        logger = logging.getLogger(f"Node-{id(self)}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s", "%H:%M:%S")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    # ---------- Подсчёт памяти ----------
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

    # ---------- Управление полями ----------
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

    # ---------- Memray ----------
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


# ---------- Example: Multi-process test ----------
def child(shared_maps, data_refs):
    node = Node(shared_maps=shared_maps)
    print("\n[Child] Reading shared data:")
    for name, uid in data_refs.items():
        val = node.heap.read(uuid.UUID(uid))
        print(f"[Child] {name}: {val}")
        if name == "field_str":
            node.heap.write(uuid.UUID(uid), "Modified by child!")


if __name__ == "__main__":
    manager = Manager()
    shared_maps = (manager.dict(), manager.dict())

    node = Node(memory_limit=3_000, shared_maps=shared_maps)
    node.start_memray_tracking("test_memray.bin")

    node.big_list = [x for x in range(3000)]
    node.field_int = 42
    node.field_str = "Hello, world!"
    node.field_list = [1, 2, 3, 4, 5]
    node.field_dict = {'a': 1, 'b': 2}
    node.field_set = {10, 20, 30}
    node.field_tuple = (1, 2, 3)

    node.stop_memray_tracking()
    node.print_memray_report("test_report.html")

    # Access same shared memory from child process
    data_refs = {k: str(v) for k, v in node.data.items()}
    p = Process(target=child, args=(shared_maps, data_refs))
    p.start()
    p.join()

    print("\n[Parent] After child modification:")
    for k, v in data_refs.items():
        print(f"  {k} = {node.heap.read(uuid.UUID(v))}")

    print("\n[MEMORY REPORT]", node.get_memory_report())
