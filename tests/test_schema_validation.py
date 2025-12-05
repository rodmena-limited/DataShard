
import pytest

from datashard.data_structures import Schema


def test_valid_schema_creation():
    valid_fields = [
        {"id": 1, "name": "id", "type": "long", "required": True},
        {"id": 2, "name": "data", "type": "string", "required": False}
    ]
    schema = Schema(schema_id=1, fields=valid_fields)
    assert schema.schema_id == 1
    assert len(schema.fields) == 2

def test_invalid_field_type():
    invalid_fields = [{"id": 1, "name": "test", "type": "super_int"}]

    with pytest.raises(ValueError, match="Invalid schema: Unknown field type 'super_int'"):
        Schema(schema_id=2, fields=invalid_fields)

def test_missing_field_properties():
    # Missing ID
    with pytest.raises(ValueError, match="Field missing required property 'id'"):
        Schema(schema_id=3, fields=[{"name": "test", "type": "string"}])

    # Missing Name
    with pytest.raises(ValueError, match="Field missing required property 'name'"):
        Schema(schema_id=3, fields=[{"id": 1, "type": "string"}])

    # Missing Type
    with pytest.raises(ValueError, match="Field missing required property 'type'"):
        Schema(schema_id=3, fields=[{"id": 1, "name": "test"}])
