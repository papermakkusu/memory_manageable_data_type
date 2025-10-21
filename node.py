import uuid
import sys
import types
import weakref
import gc
import time


class Node:
    def __init__(self, max_fields=10, memory_limit=None, warn_threshold=0.8):
        """
        Node: узел данных с контролем памяти и возможностью вложенности.
        max_fields — максимальное количество полей в узле.
        memory_limit — лимит памяти для всего дерева, байт.
        warn_threshold — доля лимита памяти (0..1), при которой выводится предупреждение.
        """
        self.max_fields = max_fields
        self.memory_limit = memory_limit
        self.warn_threshold = warn_threshold
        self.data = {}
        self.field_memory = {}  # учёт памяти по каждому полю
        self.fields_count = 0
        self.children = {}
        self.id = uuid.uuid4()
        self.memory_usage = 0
        self.total_memory_usage = 0
        self.last_gc_check = 0
        self.update_memory_usage()

    # --- Механика доступа к полям ---
    def __getattr__(self, name):
        if name not in self.data:
            self.add_field(name, None)
        return self.data[name]

    def __setattr__(self, name, value):
        if name in {'max_fields', 'memory_limit', 'warn_threshold', 'data',
                    'field_memory', 'fields_count', 'children', 'id',
                    'memory_usage', 'total_memory_usage', 'last_gc_check'}:
            super().__setattr__(name, value)
        else:
            self.add_field(name, value)

    # --- Управление памятью ---
    def _get_object_size(self, obj):
        """Рекурсивная оценка размера объекта, включая вложенные элементы."""
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
        """Обновляет использование памяти и выполняет контроль порогов."""
        self.memory_usage = sum(self.field_memory.values())
        self.total_memory_usage = self.memory_usage + sum(child.get_total_memory_usage() for child in self.children.values())

        if self.memory_limit:
            if self.total_memory_usage > self.memory_limit:
                self._raise_memory_limit_warning()
            elif self.total_memory_usage > self.memory_limit * self.warn_threshold:
                print(f"[⚠️ WARNING] Memory usage at {self.total_memory_usage} / {self.memory_limit} bytes "
                      f"({(self.total_memory_usage / self.memory_limit) * 100:.1f}%)")

    def _raise_memory_limit_warning(self):
        """Реакция на превышение лимита памяти."""
        diff = self.total_memory_usage - self.memory_limit
        print(f"[🚨 CRITICAL] Memory limit exceeded! "
              f"Limit={self.memory_limit}, Used={self.total_memory_usage}. Increasing by {diff} bytes.")
        self.memory_limit += diff

    def add_field(self, field_name, data):
        """Добавляет новое поле и пересчитывает память."""
        if self.fields_count >= self.max_fields:
            self.max_fields += 1
            print(f"[INFO] Field limit increased to {self.max_fields}")

        self.data[field_name] = data
        field_size = self._get_object_size(data)
        self.field_memory[field_name] = field_size
        self.fields_count = len(self.data)
        self.update_memory_usage()

        # Проверка на утечки/сборку мусора каждые 10 секунд
        now = time.time()
        if now - self.last_gc_check > 10:
            gc.collect()
            self.last_gc_check = now
            self.update_memory_usage()

    def remove_field(self, field_name):
        """Удаляет поле и корректирует учёт памяти."""
        if field_name in self.data:
            del self.data[field_name]
            self.field_memory.pop(field_name, None)
            self.fields_count -= 1
            self.update_memory_usage()

    def add_child(self, name, node):
        """Добавляет дочерний узел."""
        self.children[name] = node
        self.update_memory_usage()

    def get_total_memory_usage(self):
        """Возвращает суммарное использование памяти, включая потомков."""
        self.update_memory_usage()
        return self.total_memory_usage

    # --- Отчёты и профилирование ---
    def get_memory_report(self, deep=False):
        """Возвращает подробный отчёт об использовании памяти."""
        report = {
            'node_id': str(self.id),
            'fields': {k: f"{v} bytes" for k, v in self.field_memory.items()},
            'total': f"{self.total_memory_usage} bytes"
        }
        if deep and self.children:
            report['children'] = {k: v.get_memory_report(deep=True) for k, v in self.children.items()}
        return report

    def __repr__(self):
        return f"<Node id={self.id} fields={self.fields_count} mem={self.total_memory_usage}B>"

# Пример использования:

# Создаем корневой узел с ограничением на 5 полей и лимитом памяти в 10000 байт
root = Node(max_fields=5, memory_limit=1000)

# Определяем несколько функций для добавления
def greet():
    return "Hello!"

def add(a, b):
    return a + b

# Добавляем поля различных типов, включая функции
root.field_int = 42  # Целое число
root.field_str = "Hello, world!"  # Строка
root.field_list = [1, 2, 3, 4, 5]  # Список
root.field_dict = {'a': 1, 'b': 2}  # Словарь
root.field_set = {10, 20, 30}  # Множество
root.field_tuple = (1, 2, 3)  # Кортеж
root.field_greet = greet  # Функ
root.field_greet()

# Проверим, сколько памяти занимает структура
print(f"Total memory usage for root: {root.get_total_memory_usage()} bytes")

# Печатаем структуру данных
print(root)
print()
print(root.get_memory_report())
