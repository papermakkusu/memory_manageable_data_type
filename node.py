import uuid
import sys
import array
from collections import deque

class Node:
    def __init__(self, max_fields=10, memory_limit=None):
        """
        Конструктор для создания узла данных.
        max_fields — максимальное количество полей в узле.
        memory_limit — лимит на потребляемую память в байтах.
        """
        self.max_fields = max_fields  # Максимальное количество полей
        self.memory_limit = memory_limit  # Лимит памяти
        self.data = {}  # Словарь для хранения полей с данными
        self.fields_id = memoryview(bytearray(max_fields * 16))  # memoryview для хранения ID полей (16 байт на ID)
        self.fields_count = 0  # Количество текущих полей
        self.children = {}  # Словарь для хранения дочерних узлов
        self.id = uuid.uuid4()  # Уникальный ID для узла
        self.memory_usage = 0  # Память, используемая узлом
        self.total_memory_usage = 0  # Общий размер памяти всех данных в дереве
        self.update_memory_usage()

    def __getattr__(self, name):
        """Перехват доступа к полям через точечную нотацию."""
        # Если поле не существует, создаем его и возвращаем значение
        if name not in self.data:
            # Для новых полей добавляем пустое значение, которое можно будет затем изменить
            self.add_field(name, None)
        return self.data[name]

    def __setattr__(self, name, value):
        """Перехват записи в поля через точечную нотацию."""
        if name in ['max_fields', 'memory_limit', 'data', 'fields_id', 'fields_count', 'children', 'id', 'memory_usage', 'total_memory_usage']:
            # Для зарезервированных атрибутов просто устанавливаем значения
            super().__setattr__(name, value)
        else:
            # Добавляем или обновляем поле через метод add_field
            self.add_field(name, value)

    def update_memory_usage(self):
        """Обновление памяти для узла и всех дочерних данных."""
        self.memory_usage = sum(self._get_object_size(value) for value in self.data.values())
        self.total_memory_usage = self.memory_usage
        for child in self.children.values():
            self.total_memory_usage += child.total_memory_usage  # Суммируем память от дочерних узлов

        # Если превышен лимит памяти, выводим предупреждение и очищаем старые поля
        if self.memory_limit and self.total_memory_usage > self.memory_limit:
            self.clean_old_fields()

    def _get_object_size(self, obj):
        """Рекурсивное вычисление размера объекта (включая вложенные элементы для динамических типов)."""
        size = sys.getsizeof(obj)  # Основной размер объекта
        if isinstance(obj, (list, dict, set, tuple)):
            # Для коллекций учитываем их содержимое
            size += sum(self._get_object_size(item) for item in obj)
        elif isinstance(obj, str):
            # Для строк также учитываем их содержимое
            size += sys.getsizeof(obj)  # Включая строковые символы
        return size

    def add_field(self, field_name, data):
        """Добавление поля в узел с указателем на данные."""
        if self.fields_count < self.max_fields:
            # Добавляем новое поле, если не достигнут лимит
            self.data[field_name] = data
            # Добавляем ID в fields_id
            self.fields_id[self.fields_count * 16:(self.fields_count + 1) * 16] = self.generate_id_for_field(field_name)
            self.fields_count += 1
        else:
            # Логика для удаления старого поля или перераспределения памяти
            self.replace_oldest_field(field_name, data)

        self.update_memory_usage()

    def generate_id_for_field(self, field_name):
        """Генерация ID для поля (UUID, упакованный в 16 байт)."""
        return uuid.uuid5(self.id, field_name).bytes

    def replace_oldest_field(self, field_name, data):
        """Заменяем самое старое поле, если лимит превышен."""
        oldest_field_name = list(self.data.keys())[0]  # Просто пример замены первого поля
        self.data.pop(oldest_field_name)  # Удаляем старое поле
        self.add_field(field_name, data)  # Добавляем новое

    def clean_old_fields(self):
        """Очистка старых полей, если память превышает лимит."""
        while self.total_memory_usage > self.memory_limit and self.fields_count > 0:
            # Удаляем самое старое поле
            oldest_field_name = list(self.data.keys())[0]
            self.data.pop(oldest_field_name)
            self.fields_count -= 1
            self.update_memory_usage()

            # Печатаем предупреждение о превышении лимита
            print(f"WARNING: Memory usage exceeded limit ({self.memory_limit} bytes). Oldest field removed.")

    def add_child(self, path, node):
        """Добавление дочернего узла по указанному пути."""
        parts = path.split('/')
        current_node = self
        for part in parts:
            if part not in current_node.children:
                current_node.children[part] = DataNode(self.max_fields, self.memory_limit)  # Передаем лимит памяти
            current_node = current_node.children[part]
        current_node = node
        self.update_memory_usage()

    def resolve(self, path, id_map):
        """Разрешение пути через ID для доступа к данным."""
        parts = path.split('/')
        current_node = self
        for part in parts:
            if part in current_node.children:
                current_node = current_node.children[part]
            else:
                return None
        return id_map.get(current_node.id, None)

    def get_total_memory_usage(self):
        """Возвращает общий размер памяти, занимаемой узлом и его дочерними узлами."""
        return self.total_memory_usage

    def __repr__(self):
        return f"DataNode(id={repr(self.id)}, data={repr(self.data)}, memory_usage={self.memory_usage}, total_memory_usage={self.total_memory_usage}, fields_count={self.fields_count}, children={repr(self.children)})"

# Пример использования:

# Создаем корневой узел с ограничением на 5 полей и лимитом памяти в 10000 байт
root = DataNode(max_fields=5, memory_limit=10000)

# Добавляем поля различных типов
root.field_int = 42  # Целое число
root.field_str = "Hello, world!"  # Строка
root.field_list = [1, 2, 3, 4, 5]  # Список
root.field_dict = {'a': 1, 'b': 2}  # Словарь
root.field_set = {10, 20, 30}  # Множество
root.field_tuple = (1, 2, 3)  # Кортеж

# Проверим, сколько памяти занимает структура
print(f"Total memory usage for root: {root.get_total_memory_usage()} bytes")

# Печатаем структуру данных
print(root)
