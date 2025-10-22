#!/usr/bin/env python3

import uuid         # NOTE: used for unique pointers / IDs
import sys          # NOTE: for sys.getsizeof
import logging      # NOTE: logging for Heap/Node events
import os
from multiprocessing import shared_memory, Manager, Process
from heap import Heap

# NOTE: Optional memory profiler
try:
    import memray
except ImportError:
    memray = None   # HACK: silent fallback if memray is unavailable



class Node:
    """A high-level shared-memory object container built on top of Heap.

    The Node class represents a structured, reference-counted data container that stores its fields in a shared-memory
    `Heap`. It provides a dynamic, dictionary-like interface for defining and manipulating named fields whose values
    live in shared memory, allowing multiple processes to access and modify them concurrently.

    Each Node instance maintains:
      • A mapping of field names to shared-memory pointers (UUIDs)
      • Per-field memory usage and insertion order
      • Optional child Nodes for hierarchical composition
      • A shared reference to the underlying Heap metadata maps
      • Internal caches for deserialized values to reduce read overhead

    When a field is assigned (e.g., `node.value = [1, 2, 3]`), the Node automatically serializes the data and allocates
    space for it in the Heap. When read (e.g., `print(node.value)`), the value is transparently deserialized from shared
    memory (and cached if unchanged). Reference counts are tracked across all Nodes referencing the same pointer,
    allowing safe concurrent sharing and deferred cleanup via the Heap's garbage collector.

    Design:
      - Each Node owns a subset of shared-memory objects managed by a common Heap.
      - Attributes that are not internal (not starting with `_`) are interpreted as fields and automatically mapped
        to shared memory.
      - The `shared_maps` and `data_refs` interfaces enable serialization-free sharing of Node state between processes.
      - Memory usage is continuously tracked; configurable thresholds trigger warnings when approaching `memory_limit`.

    Example:
        >>> from multiprocessing import Manager
        >>> node = Node(manager=Manager())
        >>> node.message = "Hello from shared memory"
        >>> node.numbers = [1, 2, 3]

        >>> print(node.message)
        Hello from shared memory

        >>> child = Node(shared_maps=node.shared_maps, data_refs=node.data_refs)
        >>> print(child.message)
        Hello from shared memory

        >>> child.message = "Modified by child"
        >>> print(node.message)
        Modified by child
    """

    def __init__(self, max_fields=10, memory_limit=None, warn_threshold=0.8, logger=None, shared_maps=None,
                 data_refs=None, manager=None, allow_field_creation=True, use_events=True, parent_gc=True):

        # NOTE: Internal flags
        super().__setattr__('_allow_field_creation', allow_field_creation)
        super().__setattr__('_use_events', use_events)
        super().__setattr__('_parent_gc', parent_gc)

        # NOTE: bypass __setattr__ for internal attributes
        super().__setattr__('logger', logger or self._create_default_logger())
        super().__setattr__('manager', manager or Manager())

        # NOTE: create or use shared maps
        if shared_maps is None:
            shm_map = super().__getattribute__('manager').dict()
            size_map = super().__getattribute__('manager').dict()
            version_map = super().__getattribute__('manager').dict()
            refcount_map = super().__getattribute__('manager').dict()
            event_map = super().__getattribute__('manager').dict()
            free_blocks = super().__getattribute__('manager').dict()
            shared_maps = (shm_map, size_map, version_map, refcount_map, event_map, free_blocks)
        super().__setattr__('_shm_map', shared_maps[0])
        super().__setattr__('_size_map', shared_maps[1])
        super().__setattr__('_version_map', shared_maps[2])
        super().__setattr__('_refcount_map', shared_maps[3])
        super().__setattr__('_event_map', shared_maps[4])
        super().__setattr__('_free_blocks', shared_maps[5])

        # NOTE: Expose for external use
        super().__setattr__('shared_maps', shared_maps)

        # NOTE: Heap
        super().__setattr__('heap', Heap(logger=self.logger, shared_maps=shared_maps, manager=self.manager,
                                         use_events=use_events, parent_gc=parent_gc))

        # NOTE: Internal bookkeeping
        super().__setattr__('max_fields', max_fields)
        super().__setattr__('memory_limit', memory_limit)
        super().__setattr__('warn_threshold', warn_threshold)
        super().__setattr__('data', {})  # NOTE: field_name -> pointer
        super().__setattr__('field_memory', {})  # NOTE: field_name -> memory size
        super().__setattr__('field_index', {})  # NOTE: field ordering
        super().__setattr__('children', {})
        super().__setattr__('id', uuid.uuid4())
        super().__setattr__('fields_count', 0)
        super().__setattr__('memory_usage', 0)
        super().__setattr__('total_memory_usage', 0)
        super().__setattr__('last_gc_check', 0)
        super().__setattr__('_fields_id_buf', bytearray(max_fields * 16))
        super().__setattr__('fields_id', memoryview(super().__getattribute__('_fields_id_buf')))
        super().__setattr__('_memray_tracker', None)
        super().__setattr__('_memray_report_path', None)
        super().__setattr__('_cache', {})  # NOTE: field_name -> last deserialized value
        super().__setattr__('_cache_digest', {})  # NOTE: cache consistency

        self.logger.info(f"[INIT] Node {self.id} created with heap memory model")
        self.update_memory_usage()

        # NOTE: Attach pointers from parent
        if data_refs:
            super().__setattr__('data', {k: uuid.UUID(v) for k, v in data_refs.items()})

    # NOTE: Property to expose stringified references
    @property
    def data_refs(self):
        data = super().__getattribute__('data')
        return {k: str(v) for k, v in data.items()}

    # XXX: Cleanup API
    def cleanup_unused_memory(self):
        """Explicitly clean all zero-refcount memory"""
        keys = list(self.heap._refcount_map.keys())
        for key in keys:
            ref = self.heap._refcount_map.get(key, 0)
            if ref == 0:
                self.heap.free(key)

    # XXX: Overrides for safety
    def __getattr__(self, name):
        """Intercept attribute access to fetch field value from shared memory"""
        if self._is_reserved_attr(name):
            return super().__getattribute__(name)

        data = super().__getattribute__('data')
        cache = super().__getattribute__('_cache')
        cache_digest = super().__getattribute__('_cache_digest')

        if name not in data:
            raise AttributeError(f"Field '{name}' not found. Automatic creation is disabled.")

        pointer = data[name]
        key = str(pointer)
        shm_info = super().__getattribute__('heap')._shm_map.get(key)

        if shm_info is None:
            val = super().__getattribute__('heap').read(pointer)
            cache[name] = val
            cache_digest[name] = None
            return val

        if isinstance(shm_info, tuple):
            shm_name = shm_info[0]
        else:
            shm_name = shm_info
        size = super().__getattribute__('heap')._size_map.get(key)
        if size is None:
            val = super().__getattribute__('heap').read(pointer)
            cache[name] = val
            cache_digest[name] = None
            return val
        shm = shared_memory.SharedMemory(name=shm_name)
        try:
            buf = bytes(shm.buf[:size])
        finally:
            shm.close()
        digest = hash(buf)
        if name in cache and cache_digest.get(name) == digest:
            return cache[name]
        val = super().__getattribute__('heap')._deserialize(buf)
        cache[name] = val
        cache_digest[name] = digest
        return val

    def __setattr__(self, name, value):
        """Intercept attribute assignment to manage shared memory fields"""
        if self._is_reserved_attr(name):
            # NOTE: directly set internal attributes without going through Heap
            return super().__setattr__(name, value)
        if name in self.data:
            # NOTE: field exists; update its value in Heap
            self.set_field_value(name, value)
        else:
            if self._allow_field_creation:
                # NOTE: automatically add new field
                self.add_field(name, value)
            else:
                raise AttributeError(f"Cannot create field '{name}' in child Node {self.id}")
        # NOTE: invalidate cache after write
        if '_cache' in self.__dict__:
            self._cache.pop(name, None)
            self._cache_digest.pop(name, None)

    def _is_reserved_attr(self, name):
        """Check if name is a reserved internal attribute to bypass Heap"""
        reserved = {
            'max_fields', 'memory_limit', 'warn_threshold', 'data', 'field_memory', 'field_index', 'fields_count',
            'children', 'id', 'memory_usage', 'total_memory_usage', 'last_gc_check', 'logger', '_memray_tracker',
            '_memray_report_path', '_fields_id_buf', 'fields_id', 'heap', '_cache', '_cache_digest', 'add_field',
            'set_field_value', 'get_field_value', 'remove_field', 'get_memory_report', 'get_total_memory_usage',
            'start_memray_tracking', 'stop_memray_tracking', 'print_memray_report', 'export_shared_refs', 'data_refs',
            'attach_shared_refs', 'cleanup_unused_memory', '__repr__', '__getattr__', '__setattr__', '__init__',
        }
        if name.startswith('_'):
            return True
        # NOTE: Access __dict__ directly to avoid triggering __getattr__ recursion
        return name in super().__getattribute__('__dict__') or name in reserved

    # XXX: Field management
    def add_field(self, field_name, value):
        """Add a new field to the Node and allocate memory in Heap"""
        if field_name in self.data:
            return self.set_field_value(field_name, value)
        pointer = self.heap.alloc(value)
        self.data[field_name] = pointer  # NOTE: store pointer
        self.field_memory[field_name] = self._get_object_size(value)  # NOTE: track memory usage
        self.field_index[field_name] = len(self.field_index)  # NOTE: store insertion order
        self.fields_count = len(self.data)
        self.update_memory_usage()  # NOTE: refresh Node memory accounting

    def set_field_value(self, field_name, value):
        """Update existing field value in shared memory"""
        if field_name not in self.data:
            return self.add_field(field_name, value)
        pointer = self.data[field_name]
        self.heap.write(pointer, value)  # NOTE: overwrite Heap memory
        self.field_memory[field_name] = self._get_object_size(value)
        self.update_memory_usage()  # NOTE: update Node memory accounting

    def get_field_value(self, field_name):
        """Return field value from Heap"""
        pointer = self.data.get(field_name)
        return self.heap.read(pointer) if pointer else None

    def remove_field(self, field_name):
        """Remove a field, decrement refcount, and cleanup memory"""
        if field_name not in self.data:
            return
        pointer = self.data.pop(field_name)
        try:
            self.heap.dec_ref(pointer)  # NOTE: decrement reference count; GC may reclaim
        except Exception as e:
            self.logger.warning(f"[REMOVE] heap.dec_ref failed for {pointer}: {e}")
        self.field_memory.pop(field_name, None)
        self.field_index.pop(field_name, None)
        if '_cache' in self.__dict__:
            self._cache.pop(field_name, None)
            self._cache_digest.pop(field_name, None)
        self.fields_count = len(self.data)
        self.update_memory_usage()

    # XXX: Logging & helpers
    def _create_default_logger(self):
        """Default logger for Node instance"""
        logger = logging.getLogger(f"Node-{id(self)}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s", "%H:%M:%S")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def _get_object_size(self, obj):
        """Recursively estimate memory usage of object (for memory accounting)"""
        size = sys.getsizeof(obj)
        if isinstance(obj, (list, dict, set, tuple)):
            size += sum(self._get_object_size(i) for i in obj)
        elif isinstance(obj, str):
            size += len(obj.encode('utf-8'))
        elif isinstance(obj, Node):
            size += obj.get_total_memory_usage()
        return size

    def update_memory_usage(self):
        """Compute memory usage for Node fields and children"""
        # NOTE: always access via super() to avoid __getattr__ recursion
        field_memory = super().__getattribute__('field_memory')
        children = super().__getattribute__('children')
        memory_limit = super().__getattribute__('memory_limit')
        warn_threshold = super().__getattribute__('warn_threshold')
        logger = super().__getattribute__('logger')

        memory_usage = sum(field_memory.values())
        total_memory_usage = memory_usage + sum(c.get_total_memory_usage() for c in children.values())
        super().__setattr__('memory_usage', memory_usage)
        super().__setattr__('total_memory_usage', total_memory_usage)

        if memory_limit and total_memory_usage > memory_limit * warn_threshold:
            logger.warning(f"[WARN] Node {self.id} memory {total_memory_usage}/{memory_limit} bytes")

    # XXX: Reporting
    def get_memory_report(self, deep=False):
        """Return memory report for Node; deep=True includes children recursively"""
        report = {"node_id": str(self.id),
                  "fields": {k: {"size_bytes": v, "pointer": str(self.data[k]), "index": self.field_index.get(k)}
                             for k, v in self.field_memory.items()},
                  "total": f"{self.total_memory_usage} bytes"}
        if deep and self.children:
            report["children"] = {k: v.get_memory_report(deep=True) for k, v in self.children.items()}
        return report

    def get_total_memory_usage(self):
        """Return total memory usage including children"""
        self.update_memory_usage()
        return self.total_memory_usage

    def __repr__(self):
        return f"<Node id={self.id} fields={self.fields_count} mem={self.total_memory_usage}B>"

if __name__ == "__main__":
    import doctest

    doctest.testmod()
    print("Doctests completed!")
