# GPU pilot attempt 001

Status: **FAILED AFTER FIRST TREATMENT; NOT PERFORMANCE EVIDENCE**

The instrumented Spark 3.5.5 / RAPIDS revision `4c66f7214` completed the first
warm-up query, then the Python harness called a nonexistent
`SparkContext.clearJobGroup()` method. Spark stopped cleanly with exit code 1.

The completed event log and one journal record are retained. The harness fix only clears
the two local job-group properties by setting them to `None`; treatment definitions,
query, and Spark/RAPIDS controls are unchanged. The full pilot is rerun in attempt 002.
