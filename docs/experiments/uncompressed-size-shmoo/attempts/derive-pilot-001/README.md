# Derivation pilot 001

Status: rejected harness attempt.

The single-month derivation used Spark `local[16]` with the default driver heap and
failed during the repartition shuffle:

```text
java.lang.OutOfMemoryError: Java heap space
Task 7 in stage 1.0 failed; Spark aborted the write job.
```

No derived data or performance result from this attempt was accepted. Resolution:
preregister an explicit 32 GiB local driver heap for derivation only.
