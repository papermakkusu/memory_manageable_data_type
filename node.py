import uuid
import sys
import types
import weakref
import gc
import time
import logging
import os

try:
    import memray
except ImportError:
    memray = None


class Node:
    def __init__(self, max_fields=10, memory_limit=None, warn_threshold=0.8, logger=None):
        """
        Node — узел данных с контролем памяти и интеграцией с внешним логгером (например, KTT Logger).

        :param max_fields: максимум полей
        :param memory_limit: лимит памяти, байт
        :param warn_threshold: доля лимита для предупреждений
        :param logger: внешний логгер (например, solution.logger или KttLogger)
        """
        # Если логгер не передан — создаём локальный fallback
        self.logger = logger or self._create_default_logger()

        self.max_fields = max_fields
        self.memory_limit = memory_limit
        self.warn_threshold = warn_threshold
        self.data = {}
        self.field_memory = {}
        self.fields_count = 0
        self.children = {}
        self.id = uuid.uuid4()
        self.memory_usage = 0
        self.total_memory_usage = 0
        self.last_gc_check = 0

        # Новые поля для Memray
        self._memray_tracker = None
        self._memray_report_path = None

        self.logger.info(f"[INIT] Node {self.id} created with max_fields={max_fields}, memory_limit={memory_limit}")
        self.update_memory_usage()

    # ---------- Вспомогательные методы ----------
    def _create_default_logger(self):
        """Создаёт fallback-логгер, если KTT-логгер не был передан."""
        logger = logging.getLogger(f"Node-{id(self)}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%H:%M:%S"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    # ---------- Механика доступа ----------
    def __getattr__(self, name):
        if name not in self.data:
            self.add_field(name, None)
        return self.data[name]

    def __setattr__(self, name, value):
        if name in {'max_fields', 'memory_limit', 'warn_threshold', 'data', 'field_memory',
                    'fields_count', 'children', 'id', 'memory_usage', 'total_memory_usage',
                    'last_gc_check', 'logger', '_memray_tracker', '_memray_report_path'}:
            super().__setattr__(name, value)
        else:
            self.add_field(name, value)

    # ---------- Управление памятью ----------
    def _get_object_size(self, obj):
        size = sys.getsizeof(obj)
        if isinstance(obj, (list, dict, set, tuple)):
            size += sum(self._get_object_size(item) for item in obj)
        elif isinstance(obj, str):
            size += len(obj.encode('utf-8'))
        elif isinstance(obj, types.FunctionType):
            size += sys.getsizeof(obj.__code__) + sys.getsizeof(obj.__defaults__) + sys.getsizeof(obj.__closure__)
        elif isinstance(obj, Node):
            size += obj.get_total_memory_usage()
        return size

    def update_memory_usage(self):
        self.memory_usage = sum(self.field_memory.values())
        self.total_memory_usage = self.memory_usage + sum(child.get_total_memory_usage() for child in self.children.values())

        if self.memory_limit:
            ratio = self.total_memory_usage / self.memory_limit
            if ratio > 1:
                self._raise_memory_limit_warning()
            elif ratio > self.warn_threshold:
                self.logger.warning(
                    f"[WARN] Node {self.id} memory at {self.total_memory_usage} / {self.memory_limit} "
                    f"({ratio*100:.1f}%)"
                )

    def _raise_memory_limit_warning(self):
        diff = self.total_memory_usage - self.memory_limit
        self.logger.critical(
            f"[CRITICAL] Node {self.id} exceeded memory limit! "
            f"Limit={self.memory_limit}, Used={self.total_memory_usage}. Increasing by {diff} bytes."
        )
        self.memory_limit += diff
        self.logger.info(f"[MEMORY] New limit for {self.id}: {self.memory_limit} bytes")

    def add_field(self, field_name, data):
        if self.fields_count >= self.max_fields:
            self.max_fields += 1
            self.logger.info(f"[INFO] Field limit increased to {self.max_fields}")

        self.data[field_name] = data
        field_size = self._get_object_size(data)
        self.field_memory[field_name] = field_size
        self.fields_count = len(self.data)

        self.logger.info(f"[ADD] Field '{field_name}' added to {self.id} ({field_size} bytes)")
        self.update_memory_usage()

        now = time.time()
        if now - self.last_gc_check > 10:
            collected = gc.collect()
            self.logger.debug(f"[GC] Garbage collector freed {collected} objects")
            self.last_gc_check = now
            self.update_memory_usage()

    def remove_field(self, field_name):
        if field_name in self.data:
            self.logger.info(f"[REMOVE] Field '{field_name}' removed from {self.id}")
            del self.data[field_name]
            self.field_memory.pop(field_name, None)
            self.fields_count -= 1
            self.update_memory_usage()

    def add_child(self, name, node):
        self.children[name] = node
        self.logger.info(f"[CHILD] Node {node.id} added as child '{name}' to {self.id}")
        self.update_memory_usage()

    def get_total_memory_usage(self):
        self.update_memory_usage()
        return self.total_memory_usage

    def get_memory_report(self, deep=False):
        report = {
            'node_id': str(self.id),
            'fields': {k: f"{v} bytes" for k, v in self.field_memory.items()},
            'total': f"{self.total_memory_usage} bytes"
        }
        if deep and self.children:
            report['children'] = {k: v.get_memory_report(deep=True) for k, v in self.children.items()}
        return report

    # ---------- Новые методы: интеграция с Memray ----------
    def start_memray_tracking(self, path="memray_report.bin"):
        """Запускает Memray-трекер и сохраняет профиль памяти в бинарный файл."""
        if not memray:
            self.logger.warning("[MEMRAY] Memray not installed — skipping memory tracking.")
            return

        if self._memray_tracker:
            self.logger.warning("[MEMRAY] Tracker already running.")
            return

        self._memray_report_path = path
        self._memray_tracker = memray.Tracker(path)
        self._memray_tracker.__enter__()
        self.logger.info(f"[MEMRAY] Started tracking → {path}")

    def stop_memray_tracking(self):
        """Останавливает Memray-трекер."""
        if not self._memray_tracker:
            self.logger.warning("[MEMRAY] Tracker was not active.")
            return

        self._memray_tracker.__exit__(None, None, None)
        self.logger.info(f"[MEMRAY] Tracking stopped. Data saved to {self._memray_report_path}")
        self._memray_tracker = None

    def print_memray_report(self, html_path=None):
        """Генерирует HTML-отчёт с flamegraph на основе собранных данных."""
        if not memray:
            self.logger.warning("[MEMRAY] Memray not installed — cannot generate report.")
            return

        if not self._memray_report_path or not os.path.exists(self._memray_report_path):
            self.logger.warning("[MEMRAY] No report file found to generate HTML.")
            return

        html_path = html_path or (self._memray_report_path + ".html")
        os.system(f"memray flamegraph {self._memray_report_path} -o {html_path}")
        self.logger.info(f"[MEMRAY] HTML report generated: {html_path}")

    def __repr__(self):
        return f"<Node id={self.id} fields={self.fields_count} mem={self.total_memory_usage}B>"

node = Node(memory_limit=5_000_000)

node.start_memray_tracking("test_memray.bin")

# создаём нагрузку
node.add_field("big_list", [x for x in range(10_0000)])

node.stop_memray_tracking()
node.print_memray_report("test_report.html")

print(node.get_memory_report())
print()
print(f"Total memory usage for root: {node.get_total_memory_usage()} bytes")

# Добавляем поля различных типов, включая функции
node.field_int = 42  # Целое число
node.field_str = "Hello, world!"  # Строка
node.field_list = [1, 2, 3, 4, 5]  # Список
node.field_dict = {'a': 1, 'b': 2}  # Словарь
node.field_set = {10, 20, 30}  # Множество
node.field_tuple = (1, 2, 3)  # Кортеж
node.field_greet = greet  # Функ
node.field_greet()

# Проверим, сколько памяти занимает структура
print(f"Total memory usage for root: {node.get_total_memory_usage()} bytes")

# Печатаем структуру данных
print(node)
print()
print(node.get_memory_report())
