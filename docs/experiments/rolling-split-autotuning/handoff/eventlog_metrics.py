"""Metric names and GpuScan node walking, shared by every report generator.

Both the kit reports and the learning ledger read the same event logs, so the metric names and the
"is this metric present" check live here rather than in each parser. Renaming a plugin metric is
then a one-line change instead of a hunt.
"""

# Split each scan planned with.
SCAN_SPLIT_BYTES = "scan max split bytes"

# Decoded scan-output bytes (cudf-spark #15584).
BATCH_BYTES = "decoded batch bytes"
BATCH_BYTES_ALIASES = {BATCH_BYTES}

BATCHES = "output columnar batches"


def is_split_metric(name):
    return name == SCAN_SPLIT_BYTES


def canonical(name):
    """Metric name with the batch-bytes aliases folded onto one spelling."""
    return BATCH_BYTES if name in BATCH_BYTES_ALIASES else name


class MissingSplitMetric(Exception):
    """GpuScan nodes are present but none carries the split metric.

    Every report reads the chosen split from it, so this is raised rather than reporting a blank
    split column.
    """
    def __init__(self, el):
        super().__init__(
            f"{el}: GpuScan nodes carry no '{SCAN_SPLIT_BYTES}' metric.\n"
            f"  Either the plugin jar predates the metric, or the arm did not run with\n"
            f"  --conf spark.rapids.sql.metrics.level=DEBUG (the metric is DEBUG-level only).")


def load(path=None):
    """Import this module by path, for scripts that live in a different directory."""
    import importlib.util, os
    path = path or os.path.join(os.path.dirname(os.path.abspath(__file__)), "eventlog_metrics.py")
    spec = importlib.util.spec_from_file_location("eventlog_metrics", path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m
