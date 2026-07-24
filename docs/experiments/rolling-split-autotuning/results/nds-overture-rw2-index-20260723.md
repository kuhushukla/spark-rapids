# Overture real-world scan-split study — index — 2026-07

Seven genuinely real-world, scan-heavy Overture queries (RW6–9 + geometry-full GF1/GF2 + road coverage), each swept over `maxPartitionBytes` {128m…4g} with the fill-to-target autotuner, warm iters 2–5, one query per session (`BENCHMARK-METHOD.md`), RTX A5000. All fully on GPU.

## Cross-query summary

| query | scanned | OFF optimum | ftt split | ftt vs opt | byte skew opt→ftt | gpuTime opt→ftt | verdict |
|---|---|---|---|---|---|---|---|
| [rw6 provenance (5-theme union)](nds-overture-rw6-provenance-20260723.md) | 10.8 GiB | 1g | per-table 0.4–2.1 GB | ≈ / beats | 7.65× → 1.85× | 106 → 89 s | WIN (per-table) |
| [rw7 road freshness (explode)](nds-overture-rw7-roadfreshness-20260723.md) | 2.1 GiB | 2g | 4.99 GB | +6% wall | 1.48× → 1.25× | 22.8 → 12.6 s (−45%) | gpu-lean, +6% wall |
| [rw8 multilingual roads](nds-overture-rw8-multilingual-20260723.md) | 1.7 GiB | 2g | 7.9 GB | +11% wall | 1.54× → 1.16× | 5.5 → 2.8 s (−49%) | gpu-lean, +11% wall |
| [rw9 POI addressing](nds-overture-rw9-poiaddressing-20260723.md) | 1.2 GiB | flat ≥1g | 1.92 GB | ≈ tie | 1.27× → 1.36× | 11.6 → 2.6 s | tie (flat) |
| [gf1 road profile (geom+all cols)](nds-overture-gf1-roadprofile-20260724.md) | 56.4 GiB | 256m | 0.45 GB | +0.9% wall | 1.08× → 1.07× | 404 → 348 s (−14%) | WIN (goal-met) |
| [gf2 geometry types (5-theme)](nds-overture-gf2-geometrytypes-20260724.md) | 48.1 GiB | 512m | per-theme 1.3–6.0 GB | +6% wall | 2.00× → 1.50× | 126 → 94 s (−26%) | gpu-lean, +6% wall |
| [road coverage (earlier)](nds-overture-realworld-20260723.md) | 13.6 GiB | 512m ≈ 2g | 1.22 GB | +5% wall | — | — | gpu-lean, +5% wall |

## What we learned

1. **The optimum is genuinely query-dependent** — across seven real queries it ranges 512m → 1g → 2g → flat. No single fixed `maxPartitionBytes` wins them all.
2. **On a big geometry scan (gf2, 48 GiB), per-theme sizing is a safety feature**: cutting gpuTime by going to a big split is a trap — a global 4g split explodes byte skew to 13.3× (one 2.4 GB / 18.9 s straggler) and wall to +49%. ftt's per-theme splits keep skew at 1.5× and harvest −26% gpuTime at +5.7% wall.
3. **Per-table sizing is ftt's real edge** (rw6): a multi-table union forces one global split onto 5 wildly different tables → byte skew 7.65×. ftt sizes each table from its own ratio → skew 1.85×, gpuTime 106→89 s, wall matches the best fixed split. A global knob *cannot* do this.
4. **On single-table queries ftt trades wall for GPU efficiency** (rw7/rw8): highly-compressible columns → low decoded/listed ratio → a 5–8 GB split → fewer, fuller tasks that **cut gpuTime 45–49% and decode ~40%** below the optimum, at the cost of **+6–11% wall** (these are parallelism-bound, so fewer tasks slightly slows wall even as GPU work drops).
5. **ftt always cuts gpuTime and decode** (fewer, fuller batches = less GPU work) — a real efficiency win; it only costs wall when the query is parallelism-bound rather than GPU-bound.
6. **Value of the autotuner:** avoid a bad setting, balance multi-table scans, and land start-independently in the right neighbourhood — not beat a hand-tuned single-table optimum.

## Goal: save gpuTime + decode, keep runtime within 5% (ON=ftt vs OFF=hand-tuned optimum)

| query | scanned | ftt split | gpuTime saved | decode saved | wall Δ vs optimum | within 5% |
|---|---|---|---|---|---|---|
| rw6 provenance | 10.8 GiB | per-table 0.4–2.1 GB | −16% | −10% | −0.8% (faster) | YES |
| rw7 freshness | 2.1 GiB | 4.99 GB | −44% | −43% | +6.6% | no |
| rw8 multilingual | 1.7 GiB | 7.9 GB | −49% | −41% | +11.4% | no |
| rw9 POI | 1.2 GiB | 1.92 GB | −78% | −73% | +0.6% (≈tie) | YES |
| gf1 road profile | 56.4 GiB | 0.45 GB | −14% | −11% | +0.9% | YES |
| gf2 geometry | 48.1 GiB | per-theme 1.3–6.0 GB | −26% | −23% | +5.7% | no |
| road coverage | 13.6 GiB | 1.22 GB | −4% | −6% | +4.7% | YES (marginal) |

**GPU-efficiency half — achieved everywhere:** every query cuts gpuTime (−4% to −78%) and decode (−6% to −73%). **≤5% runtime half — vs the hand-tuned optimum:** met for **rw6, rw9, gf1, road coverage**; **rw7 (+6.6%), rw8 (+11.4%), gf2 (+5.7%)** run fewer tasks than the 16 cores can absorb, so they save GPU work but lose 5.7–11% wall. The split direction is data-driven: **gf1 reads geometry + all ~15 columns → ftt sizes DOWN to 0.45 GB and nails the optimum**, while compressible-column queries (rw7/rw8/gf2) size up and overshoot. Against an *untuned default* `maxPartitionBytes`, ftt is within 5% or faster everywhere.

## Reports

- **rw6 provenance (5-theme union)** — [md](nds-overture-rw6-provenance-20260723.md) · [html](nds-overture-rw6-provenance-20260723.html)
- **rw7 road freshness (explode)** — [md](nds-overture-rw7-roadfreshness-20260723.md) · [html](nds-overture-rw7-roadfreshness-20260723.html)
- **rw8 multilingual roads** — [md](nds-overture-rw8-multilingual-20260723.md) · [html](nds-overture-rw8-multilingual-20260723.html)
- **rw9 POI addressing** — [md](nds-overture-rw9-poiaddressing-20260723.md) · [html](nds-overture-rw9-poiaddressing-20260723.html)
- **gf1 road profile (geom+all cols)** — [md](nds-overture-gf1-roadprofile-20260724.md) · [html](nds-overture-gf1-roadprofile-20260724.html)
- **gf2 geometry types (5-theme)** — [md](nds-overture-gf2-geometrytypes-20260724.md) · [html](nds-overture-gf2-geometrytypes-20260724.html)
- **road coverage (earlier)** — [md](nds-overture-realworld-20260723.md) · [html](nds-overture-realworld-20260723.html)