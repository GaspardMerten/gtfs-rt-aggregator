"""
Build Arrow tables straight from GTFS-RT protobuf messages, without going
through dicts.

A builder tree mirrors the Arrow schema of a service: struct fields read the
matching protobuf field (by its JSON name, e.g. tripId), lists read repeated
fields, and leaves append to Python lists that become Arrow arrays at the end.
Enums become their names, like MessageToDict. About 3 times faster than
MessageToDict + pa.Table.from_pylist on trip updates.

Two things keep the per-value work small:
- a missing struct only records a False flag: its children are filled for
  present rows only, and expanded with one vectorized take() at the end;
- values are appended with the list's own (C) append method, not through a
  Python function per value.
"""

import keyword
from functools import partial
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc
from google.protobuf.descriptor import Descriptor, FieldDescriptor

_MESSAGE_TYPES = (FieldDescriptor.TYPE_MESSAGE, FieldDescriptor.TYPE_GROUP)


def _is_repeated(field: FieldDescriptor) -> bool:
    repeated = getattr(field, "is_repeated", None)
    if repeated is not None:
        return repeated() if callable(repeated) else repeated
    return field.label == FieldDescriptor.LABEL_REPEATED


def _read(name: str) -> str:
    """Python expression reading a field of message."""
    if name.isidentifier() and not keyword.iskeyword(name):
        return f"message.{name}"
    return f"getattr(message, {name!r})"


_UNSIGNED_64 = (FieldDescriptor.TYPE_UINT64, FieldDescriptor.TYPE_FIXED64)
_INT64_MAX = 2**63 - 1


class _Leaf:
    """A scalar column."""

    def __init__(self, arrow_type: pa.DataType, proto_field: Optional[FieldDescriptor]):
        self.type = arrow_type
        self.values: List[Any] = []
        append = self.values.append
        self.add_null = partial(append, None)
        if proto_field is not None and proto_field.type == FieldDescriptor.TYPE_ENUM:
            names = {v.number: v.name for v in proto_field.enum_type.values}
            self.add = lambda number: append(names.get(number, number))
        elif proto_field is not None and proto_field.type in _UNSIGNED_64:
            # Stored signed (no unsigned types): out-of-range values are nonsense
            self.add = lambda value: append(value if value <= _INT64_MAX else None)
        else:
            self.add = append

    def finish(self) -> pa.Array:
        return pa.array(self.values, type=self.type)


class _Struct:
    """A message: one child builder per Arrow field, filled for present rows only."""

    def __init__(self, arrow_type: pa.StructType, descriptor: Optional[Descriptor]):
        self.type = arrow_type
        self.valid: List[bool] = []
        self.add_null = partial(self.valid.append, False)
        self.children = []
        # (protobuf field name, repeated, add value, add null) per child
        self.readers = []
        by_json = {f.json_name: f for f in descriptor.fields} if descriptor else {}
        for index in range(arrow_type.num_fields):
            arrow_field = arrow_type.field(index)
            proto_field = by_json.get(arrow_field.name)
            child = _builder(arrow_field.type, proto_field)
            self.children.append(child)
            self.readers.append(
                (
                    proto_field.name if proto_field is not None else None,
                    proto_field is not None and _is_repeated(proto_field),
                    child.add,
                    child.add_null,
                )
            )

        self.add = self._compile()

    def _compile(self):
        """
        A function reading this message type's fields one after the other,
        with their names written out: no loop over field descriptions per message.
        """
        lines = ["def add(message):", "    valid_append(True)"]
        scope = {"valid_append": self.valid.append}
        has_optional = any(
            name and not repeated for name, repeated, _, _ in self.readers
        )
        if has_optional:
            lines.append("    has = message.HasField")
        for index, (name, repeated, add, add_null) in enumerate(self.readers):
            scope[f"add{index}"] = add
            scope[f"null{index}"] = add_null
            if name is None:
                lines.append(f"    null{index}()")
            elif repeated:
                lines.append(f"    add{index}({_read(name)})")
            else:
                lines.append(f"    if has({name!r}):")
                lines.append(f"        add{index}({_read(name)})")
                lines.append("    else:")
                lines.append(f"        null{index}()")
        exec("\n".join(lines), scope)
        return scope["add"]

    def finish(self) -> pa.Array:
        children = [child.finish() for child in self.children]
        valid = pa.array(self.valid, pa.bool_())
        if not all(self.valid):
            # Children hold present rows only: spread them, nulls elsewhere
            # (row i takes child row "present rows before i")
            positions = pc.subtract(
                pc.cumulative_sum(pc.cast(valid, pa.int64())), pa.scalar(1, pa.int64())
            )
            indices = pc.if_else(valid, positions, pa.scalar(None, pa.int64()))
            children = [pc.take(child, indices) for child in children]
        return pa.StructArray.from_arrays(
            children, fields=list(self.type), mask=pc.invert(valid)
        )


class _List:
    """A repeated field."""

    def __init__(self, arrow_type: pa.ListType, proto_field: Optional[FieldDescriptor]):
        self.type = arrow_type
        self.offsets = [0]
        self.valid: List[bool] = []
        self.child = _builder(arrow_type.value_type, proto_field)

    def add(self, values):
        add = self.child.add
        count = 0
        for value in values:
            add(value)
            count += 1
        # An empty repeated field reads as null, like a missing key in MessageToDict
        self.valid.append(count > 0)
        self.offsets.append(self.offsets[-1] + count)

    def add_null(self):
        self.valid.append(False)
        self.offsets.append(self.offsets[-1])

    def finish(self) -> pa.Array:
        return pa.ListArray.from_arrays(
            pa.array(self.offsets, pa.int32()),
            self.child.finish(),
            type=self.type,
            mask=pc.invert(pa.array(self.valid, pa.bool_())),
        )


def _builder(arrow_type: pa.DataType, proto_field: Optional[FieldDescriptor]):
    if pa.types.is_list(arrow_type):
        return _List(arrow_type, proto_field)
    if pa.types.is_struct(arrow_type):
        descriptor = (
            proto_field.message_type
            if proto_field is not None and proto_field.type in _MESSAGE_TYPES
            else None
        )
        return _Struct(arrow_type, descriptor)
    return _Leaf(arrow_type, proto_field)


class TableBuilder:
    """
    Builds the table of one service type from protobuf messages.

    @param schema: Arrow schema of the service (before flattening)
    @param descriptor: Protobuf descriptor of the service's message (e.g. TripUpdate)
    @param row_fields: Columns not read from the message: entityId and
        contentHash (given per row) and the others (same value for every row)
    """

    def __init__(self, schema: pa.Schema, descriptor: Descriptor, row_fields):
        self.schema = schema
        self.row_fields = set(row_fields)
        message_fields = [f for f in schema if f.name not in self.row_fields]
        self.message = _Struct(pa.struct(message_fields), descriptor)
        self.entity_ids: List[str] = []
        self.hashes: List[str] = []

    def add(self, entity_id: str, content_hash: str, message):
        self.entity_ids.append(entity_id)
        self.hashes.append(content_hash)
        self.message.add(message)

    def finish(self, constants: Dict[str, Any]) -> pa.Table:
        rows = len(self.entity_ids)
        struct = self.message.finish()
        columns = {}
        for field in self.schema:
            if field.name == "entityId":
                columns[field.name] = pa.array(self.entity_ids, field.type)
            elif field.name == "contentHash":
                columns[field.name] = pa.array(self.hashes, field.type)
            elif field.name in self.row_fields:
                columns[field.name] = pa.array(
                    [constants.get(field.name)] * rows, field.type
                )
            else:
                columns[field.name] = struct.field(field.name)
        return pa.table(columns, schema=self.schema)
