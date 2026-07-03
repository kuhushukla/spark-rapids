# Full shmoo attempt 001

Status: **ABORTED DURING WARM-UP; NOT PERFORMANCE EVIDENCE**

Ten of twelve warm-ups completed. The 2011 `variable_width` warm-up then failed before
a journal record with `SchemaColumnConvertNotSupportedException`: the derived 2011
Parquet files store `payment_type` as INT64, while the frozen harness requested string.

A schema probe confirmed that this column is string in 2009/2010 and long in 2011.
Parquet/Spark does not automatically up-cast INT64 to string at the reader boundary.
The corrected protocol reads the physical epoch type and applies an explicit Spark cast
above the scan to obtain the canonical string query value. This preserves the schema
evolution as a model feature instead of hiding it. The schedule, treatment order, and
all other controls remain unchanged for attempt 002.
