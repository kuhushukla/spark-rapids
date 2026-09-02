# Data-window split learning on clickstream — results

**Run:** `/data/window-learning-20260824`. **Report:** `results/window-report-window-learning-20260824.{html,md}`.
**Config:** `handoff/windows.yaml`. **Served:** http://10.28.9.183:8000/window-report-window-learning-20260824.html

> **Measured against the table-only history key (pre-2026-09).** The plugin now keys on
> `(table, columns, filters)`. Partition predicates never enter the key, so the window
> mechanism these results test is unchanged and the numbers stand. Any statement here about
> cross-*query* sharing describes the older key. See `README.md` §1.

**Status: complete.** 154 arms, 9 jobs swept, 11 learning sequences, **0 arms excluded** — every value
below comes from an arm that ran all 5 iterations with no fatal error (enforced by `arm_health()` in
`build_ledger.py`; failing arms are named in the report and their numbers withheld).

---

## 1. The two questions

**Q1 — same query, different data window.** cs02 learns a split on months A; is that split still right
for the same query on months B?

**Q2 — different query.** csH3 learns a split; is it right for cs02 on the same table?

**Acceptance band: a candidate split passes if it is within ~9% of this job's own swept optimum on
BOTH wall and gpuTime.** Negative drift (a saving) always passes. (Set 2026-08-24.)

## 2. Why the questions are even possible

`GpuFileSourceScanExec.scala`
- 585-587 `label` = `tableIdentifier` else `rootPaths.headOption` — the table ROOT, **not** pruned
- 588 `listedBytes` = `dynamicallySelectedPartitions` — **post** partition-pruning

`ScanSplitAutotuner.scala`
- 130-131 `latestFor` filters on `tableLabel` only, `lastOption` — last writer wins
- 304 `tableLabel` = catalog name or root path
- 35-55 `ScanSplitRecord` stores listed/decoded/read bytes and the split — **no window, no predicate**

So every window of a partitioned table shares ONE history slot, and a record cannot say which window
or which query wrote it. Verified live (precheck, 2026-08-24): W1/W2/W3 all report
`table=file:/data/wiki-clickstream/parquet-part` byte-identical with listed_bytes 57.3/88.4/145.7 GB.

## 3. Method

| | |
|---|---|
| data | `/data/wiki-clickstream/parquet-part`, partitioned by (wiki, ym), 1797 partitions, 7,184,568,074 rows — identical row count to the unpartitioned copy |
| windows | equal *duration*, natural volume: W1 ≤2020-08 (34 mo, 35.9 GiB), W2 2020-09..2023-06 (34 mo, 42.7 GiB), W3 ≥2023-07 (36 mo, 57.1 GiB) |
| reference | each job's **own swept optimum** = lowest warm-median wall over 128m/256m/384m/512m/768m/1g/2g. Never a configured constant. |
| arms | off (autotuner disabled) / shared (history carries step 1's record) / iso (empty history). One application per step, so execution position is equal. |
| refine | every split the learning arms chose is re-run as a **pinned sweep arm**, so inherited and own-learnt splits have warm medians comparable to the optimum. Without this the inherited split exists only in one cold iteration. |
| iterations | 5, warm = 2..5. filecache off in every arm. |

Precheck asserts the windows partition the table exactly: 2,051,357,124 + 2,314,413,914 +
2,818,797,036 = 7,184,568,074.

## 4. Measured noise floor (28 sweep arms, 4 warm iters each)

| metric | median CoV | p90 CoV | n needed for ±2% CI |
|---|---|---|---|
| wall | 1.7% | 5.1% | 3 → 25 |
| task time | 1.7% | 5.0% | 3 → 24 |
| gpuTime | 1.9% | 5.0% | 3 → 24 |
| decode | 2.8% | 6.8% | 8 → 45 |

4 warm iterations is adequate for a typical arm, thin for the noisy tail. Differences under ~2% on
wall should not be read as real.

## 5. The data drifts substantially across windows

cs02 reads 3 of 4 columns (drops `current`). Bytes actually read per scan node:

| job | window listed | read | read/listed |
|---|---|---|---|
| cs02@W1 | 35.9 GiB | 23.77 | **66.2%** |
| cs02@W2 | 42.7 GiB | 30.54 | **71.5%** |
| cs02@W3 | 57.1 GiB | 23.29 | **40.8%** |

Same query, same columns, **1.75x swing in read selectivity**. W3 is 59% larger than W1 yet cs02
reads *less* from it in absolute terms — `current` is 33.8% of W1 but 59.2% of W3. The table's column
composition changed materially after 2023-07.

That drift moves the optimum: cs02's is 384m on W1 and W2, 512m on W3.

## 6. Results — all candidates measured as pinned sweep arms (warm medians)

Drift is vs that job's own optimum. **Bold = outside the 9% band.**

| test | job | candidate | split | wall drift | gpuTime drift | verdict |
|---|---|---|---|---|---|---|
| adjacent drift | cs02@W2 | optimum | 384m | — | — | |
| | | inherited from own@W1 | 477m | +8.2% | −15.2% | pass |
| | | own-learnt | 495m | +7.9% | −15.6% | pass |
| | | spark 128m default | 128m | **+13.5%** | **+26.1%** | fail |
| adjacent drift | cs02@W3 | optimum | 512m | — | — | |
| | | inherited from own@W2 | 495m | +1.1% | +0.1% | pass |
| | | own-learnt | 593m | +3.4% | −15.4% | pass |
| | | spark 128m default | 128m | **+26.1%** | **+63.3%** | fail |
| distant drift | cs02@W3 | inherited from own@W1 | 477m | +1.3% | +2.1% | pass |
| | | own-learnt | 593m | +3.4% | −15.4% | pass |
| cross-query | cs02@W2 | inherited from csH3@W1 | 296m | +5.6% | **+13.3%** | **FAIL (gpuTime)** |
| | | own-learnt | 495m | +7.9% | −15.6% | pass |

### Findings

1. **Every WINDOW transfer passes on both metrics.** Wall +1.1% to +8.2%, gpuTime +0.1% to +2.1% or
   negative. The one failure is cross-query, not cross-window.
2. **Inheritance is never worse than self-learning, and sometimes better.** On W3 the inherited 495m
   (+1.1%) beat the query's own freshly-computed 593m (+3.4%).
3. **Distance does not grow with temporal distance.** W1→W3 (+1.3%) did as well as W2→W3 (+1.1%).
4. **Cross-query transfer is the one failure, and it fails on gpuTime.** csH3's 296m gave cs02@W2
   +5.6% wall — inside the band, and better than cs02's own 495m at +7.9% — but **+13.3% gpuTime**,
   outside it, where the self-learnt value *saves* 15.6%. csH3 reads all four columns (ratio 3.126)
   while cs02 reads three (1.939), so csH3's split is sized for a much heavier decode and leaves
   cs02 with too many small tasks. This is the clean separation between the two questions:
   **window drift transfers safely, cross-query projection difference does not.**
5. **Spark's 128m default fails everywhere**, +13.5% to +26.1% wall and up to +63.3% gpuTime. Every
   learnt or inherited value is far better than the stock default.
6. **The autotuner systematically overshoots the wall optimum** — own-learnt splits are 16-29% larger
   than optimum in split terms. This follows from the formula: `split = batchSizeBytes / ratio`
   targets one decoded batch per task, which is not the wall minimum. Fresh data makes it more
   confidently wrong: W3's lower selectivity (40.8%) yields a lower ratio and thus a larger split.
7. **Split distance overstates differences.** On cs02@W2, 477m and 495m are 3.8% apart in split and
   0.2% apart in wall. Report performance drift; treat distance as secondary.


## 6a. All nine jobs — swept optima (the reference every comparison uses)

Warm medians (iters 2-5), grid 128m/256m/384m/512m/768m/1g/2g. `read/listed` uses actual `listedBytes`
from the plugin (W1 34.2, W2 41.3, W3 60.3 GiB). Ordered by scan intensity.

| job | wall optimum | wall s | gpuTime optimum | gpuTime s | read GiB | read/listed |
|---|---|---|---|---|---|---|
| csH3@W1 | 256m | 16.19 | 2048m | 104.94 | 103.3 | 302.0% |
| csH3@W2 | 256m | 19.16 | 2048m | 126.53 | 124.8 | 302.1% |
| csH3@W3 | 512m | 33.80 | **384m** | 149.42 | 182.2 | 302.1% |
| cs04@W1 | 296m | 8.57 | 2048m | 35.11 | 34.4 | 100.6% |
| cs04@W2 | 256m | 10.74 | 2048m | 40.95 | 41.6 | 100.7% |
| cs04@W3 | **1024m** | 15.77 | 2048m | 65.42 | 60.7 | 100.7% |
| cs02@W1 | 384m | 4.31 | 2048m | 25.12 | 23.8 | 69.5% |
| cs02@W2 | 384m | 5.09 | 2048m | 31.85 | 30.5 | 73.9% |
| cs02@W3 | 512m | 5.20 | 2048m | 38.80 | 23.3 | 38.6% |

Observations from this table alone:

- The wall optimum moves with the window for every query: csH3 256/256/512m, cs04 296/256/**1024m**,
  cs02 384/384/512m.
- gpuTime's optimum is 2048m (the largest split swept) in 8 of 9 jobs. csH3@W3 is the only job where
  gpuTime has an interior optimum (384m) rather than falling monotonically.
- `read/listed` is near-constant within a query across windows for csH3 (~302%, four columns read 3x
  via UNION ALL) and cs04 (~101%, four columns), but moves 38.6-73.9% for cs02, the only query that
  omits a column.

## 6b. Sequence coverage

11 sequences at time of writing, 2 more running (cs04 as cross-query recipient).

| sequence | tests |
|---|---|
| csH3@W1 -> csH3@W2, csH3@W2 -> csH3@W3 | Q1, scan-heavy query |
| cs04@W1 -> cs04@W2 | Q1, filtered query |
| cs02@W1 -> cs02@W2, cs02@W2 -> cs02@W3, cs02@W1 -> cs02@W3 | Q1, incl. adjacent vs distant |
| csH3@W1 -> cs02@W1 | Q2, same window (clean) |
| cs04@W1 -> cs02@W1 | Q2, same window, second donor |
| cs04@W1 -> csH3@W1 | Q2, near-identical ratios (control) |
| cs02@W1 -> csH3@W1 | Q2, reverse direction (low ratio -> high) |
| csH3@W1 -> cs02@W2 | Q2 confounded: query AND window both change |

Asymmetry to note: cs04 was a donor twice but never a recipient until the two runs added 2026-08-24,
so nothing measured how the filtered query responds to another query's split.

## 7. Wall and gpuTime disagree about the optimum

Every job's gpuTime and decode keep falling past the wall optimum, out to 2048m:

| job | wall optimum | gpuTime at 2048m vs wall-opt |
|---|---|---|
| cs02@W1 | 384m | −33.4% |
| cs02@W2 | 384m | (falls to 1g+) |
| cs02@W3 | 512m | −21.1% |
| csH3@W1 | 256m | −41.5% |

So "optimum" depends on the objective. csH3@W1 is the sharpest case: 256m is best on wall (every
other split is +12% to +19%) but 2048m halves decode. The autotuner's target sits on the GPU-saving
side, which is why it looks mediocre on wall and good on GPU occupancy at the same time.

## 8. Coverage and what it cannot answer

Measured expansion ratios for the whole clickstream suite:

| query | read_sel | decode_exp | ratio | scan nodes |
|---|---|---|---|---|
| cs03 | 0.492 | 3.227 | 1.588 | 1 |
| cs02 | 0.569 | 3.409 | 1.939 | 1 |
| csH | 0.991 | 2.674 | 2.649 | 1 |
| cs01 | 0.9995 | 3.127 | 3.126 | 1 |
| csH3 | 0.9995 | 3.127 | 3.126 | 3 |

Only **4 distinct ratios** — cs01 and csH3 coincide because both read all four columns. The table has
4 columns and 2 carry 93% of the bytes, so the projection space is nearly exhausted.

**Gap:** every one of these queries has `PushedFilters=[]` and `DataFilters=[]` — none filters rows.
So the only mechanism producing ratio differences is column projection. `cs04` was added to close
this (`WHERE link_type='link'`, a real question: which articles draw traffic from internal wiki links
rather than external search). Whether a row predicate moves the learnt split **at all** is unmeasured
as of this writing — it may change decode rather than bytes read, or nothing.

Until cs04 lands, Q2 is answered as *"when queries differ in which columns they read"*, not
*"when queries differ"*.

## 9. Reproducing

```bash
cd docs/experiments/table-split-learning/handoff
bash run_window_bench.sh check     # env + config + data assertions
bash run_window_bench.sh all       # check, sweep, learn, refine, report
bash phase_report.sh               # status + current tables, safe mid-run
```

Windows, grid, sweep jobs and sequences are all declared in `windows.yaml`; predicates are built by
`wincfg.py`, never typed on a command line, so the sweep and the learning runs always use identical
data.
