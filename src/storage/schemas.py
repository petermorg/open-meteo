from pyiceberg.types import StringType, NestedField, FloatType, TimestampType
from pyiceberg.schema import Schema


schema = Schema(
    NestedField(field_id=1, name='forecast_time', field_type=TimestampType(), required=True),
    NestedField(field_id=2, name='value', field_type=FloatType(), required=True),
    NestedField(field_id=3, name='variable_name', field_type=StringType(), required=True),
    NestedField(field_id=4, name='location_name', field_type=StringType(), required=True),
    NestedField(field_id=5, name='extract_time', field_type=TimestampType(), required=True),
)
