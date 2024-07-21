import json
from typing import Union
from pyspark.sql.types import StructType


def map_avro_type(type_name: str) -> Union[str, dict]:
    """Map a type name to an Avro compatible type name"""
    types = {
        "integer": "int",
        "bigint": "long",
        "date": {"type": "int", "logicalType": "date"}
    }
    return types.get(type_name, type_name)


def convert_schema_to_avro(schema: StructType) -> str:
    """Create a compatible Avro schema from a Spark schema"""
    json_schema = {
        "type": "record",
        "name": "topLevelRecord",
        "fields": [
            {
                "metadata": {},
                "name": column.name,
                "nullable": True,
                "type": ["null", map_avro_type(column.dataType.typeName())],
                "default": None
            }
            for column in schema
        ]
    }
    return json.dumps(json_schema)
