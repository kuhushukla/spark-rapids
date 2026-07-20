import json, sys, os
# Per mode event log: sum task-accumulable "GPU decode time" / "scan time" / "buffer time" (nanos)
# across all tasks and all iterations; report device-time totals and an approx decode throughput vs
# the decoded-bytes total (~99.6 GiB/iter for query9 store_sales, x 5 iters). Usage:
#   python3 decode_tput.py data/poc-off-results/eventlog-test-1 data/poc-listed-results/eventlog-test-1 ...

def parse(path):
    byname = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except Exception:
                continue
            if e.get("Event", "") != "SparkListenerTaskEnd":
                continue
            for a in e.get("Task Info", {}).get("Accumulables", []):
                nm = a.get("Name"); val = a.get("Update")
                if nm is None or val is None:
                    continue
                try:
                    val = int(val)
                except Exception:
                    continue
                byname[nm] = byname.get(nm, 0) + val
    return byname

for path in sys.argv[1:]:
    mode = os.path.basename(os.path.dirname(path)).replace("poc-", "").replace("-results", "")
    v = parse(path)
    dec = v.get("GPU decode time", 0)
    scan = v.get("scan time", 0)
    buf = v.get("buffer time", 0)
    print(f"== {mode} ==")
    print(f"  GPU decode time = {dec/1e9:8.1f} s   scan time = {scan/1e9:8.1f} s   buffer = {buf/1e9:7.1f} s")
    tot = 99.6 * 5  # ~GiB decoded across 5 iterations
    if dec > 0:
        print(f"  approx decode throughput (~{tot:.0f} GiB decoded) = {tot/(dec/1e9):.1f} GiB/s (device-time)")
