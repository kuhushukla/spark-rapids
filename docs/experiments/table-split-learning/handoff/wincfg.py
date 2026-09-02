#!/usr/bin/env python3
"""windows.yaml -> SQL predicates. Single source of truth for both the sweep and the learning runs.

Predicates are BUILT here, never typed in the config, and are emitted space-free because they travel
through conf strings that get word-split.
"""
import sys, os, yaml

HERE = os.path.dirname(os.path.abspath(__file__))
CFG = yaml.safe_load(open(os.environ.get("WINCFG", os.path.join(HERE, "windows.yaml"))))


def pred(name):
    w = CFG["windows"].get(name)
    if w is None:
        sys.exit(f"unknown window: {name}")
    col = w.get("column", CFG["partition_column"])
    if "eq" in w:
        return f"{col}='{w['eq']}'"
    if "ne" in w:
        return f"{col}<>'{w['ne']}'"
    lo, hi = w.get("from"), w.get("to")          # half-open [lo, hi)
    if lo and hi:
        return f"({col}>='{lo}')and({col}<'{hi}')"
    if lo:
        return f"{col}>='{lo}'"
    if hi:
        return f"{col}<'{hi}'"
    return ""                                     # whole table


def seq_name(s):
    """Self-describing arm name, so a directory listing reads as the experiment."""
    return f"{s['q1']}@{s['w1']}_to_{s['q2']}@{s['w2']}"


def check_cover():
    """Assert the `cover` windows are contiguous half-open ranges with no gap or overlap."""
    names = CFG.get("cover", [])
    rs = []
    for n in names:
        w = CFG["windows"][n]
        if "eq" in w or "ne" in w:
            return f"cover window {n} is categorical; cannot check contiguity"
        rs.append((n, w.get("from"), w.get("to")))
    if not rs:
        return "no cover defined"
    if rs[0][1] is not None:
        return f"cover does not start open-ended: {rs[0][0]} has from={rs[0][1]}"
    if rs[-1][2] is not None:
        return f"cover does not end open-ended: {rs[-1][0]} has to={rs[-1][2]}"
    for (an, _, ato), (bn, bfrom, _) in zip(rs, rs[1:]):
        if ato != bfrom:
            return f"gap/overlap between {an}(to={ato}) and {bn}(from={bfrom})"
    return None


if __name__ == "__main__":
    a = sys.argv[1]
    if a == "pred":
        print(pred(sys.argv[2]))
    elif a == "windows":
        print(" ".join(CFG["windows"]))
    elif a == "cover":
        print(" ".join(CFG.get("cover", [])))
    elif a == "grid":
        print(" ".join(str(g) for g in CFG["grid"]))
    elif a == "sweep_jobs":
        for j in CFG["sweep_jobs"]:
            print(f"{j['query']} {j['window']}")
    elif a == "sequences":
        for s in CFG["sequences"]:
            print(f"{seq_name(s)} {s['q1']} {s['w1']} {s['q2']} {s['w2']} {s.get('tests','-')}")
    elif a == "check_cover":
        e = check_cover()
        print(e or "OK")
        sys.exit(1 if e else 0)
    else:
        print(CFG.get(a, ""))
