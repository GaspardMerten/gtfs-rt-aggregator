"""
Fast protobuf message -> dict conversion for GTFS-RT entities.

Same keys and values as google.protobuf.json_format.MessageToDict (JSON field
names, enum names), except that 64-bit integers stay ints instead of becoming
strings, and extensions are left out. About 3 times faster: MessageToDict is
written for JSON output and spends most of its time on checks that do not
apply here.
"""

import base64

from google.protobuf.descriptor import FieldDescriptor

_MESSAGE = FieldDescriptor.TYPE_MESSAGE
_GROUP = FieldDescriptor.TYPE_GROUP
_ENUM = FieldDescriptor.TYPE_ENUM
_BYTES = FieldDescriptor.TYPE_BYTES


def _is_repeated(field) -> bool:
    repeated = getattr(field, "is_repeated", None)
    if repeated is not None:
        return repeated() if callable(repeated) else repeated
    return field.label == FieldDescriptor.LABEL_REPEATED


def message_to_dict(message) -> dict:
    result = {}
    for field, value in message.ListFields():
        if field.is_extension:
            continue
        if _is_repeated(field):
            result[field.json_name] = [_value(field, item) for item in value]
        else:
            result[field.json_name] = _value(field, value)
    return result


def _value(field, value):
    kind = field.type
    if kind == _MESSAGE or kind == _GROUP:
        return message_to_dict(value)
    if kind == _ENUM:
        enum_value = field.enum_type.values_by_number.get(value)
        return enum_value.name if enum_value is not None else value
    if kind == _BYTES:
        return base64.b64encode(value).decode("ascii")
    return value
