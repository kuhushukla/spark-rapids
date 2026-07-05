<!--
Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Amendment 001: post-run hardening and exploratory analysis

Date: 2026-07-04 America/Chicago

Treatment data had been examined before this amendment. It does not modify the
frozen run, schedule, thresholds, or primary result.

Changes for future replication are in `scripts/run_experiment_v2.sh` and
`scripts/analyze_v2.py`; frozen originals remain unchanged:

- the wrapper now executes the frozen inventory validator immediately before
  Spark;
- the primary analyzer handles a one-window smoke run by failing prediction
  gates rather than comparing `None` with a threshold;
- `scripts/analyze_exploratory.py` compares prospective-only history estimators,
  drift diagnostics, treatment-order effects, metric invariance, and bootstrap
  block-length sensitivity;
- the Scala POC distinguishes planning-time listed encoded bytes from
  post-execution compressed bytes read, makes codec non-gating, and uses the
  latest compatible same-table ratio for the point prediction.

Run `run-001` executed the preregistered hashes recorded in its provenance.
Its [postflight validation](../analysis/postflight-validation.json) confirmed
that the frozen paths, sizes, and mtimes still matched after execution. The new wrapper preflight was not part of that
run and must not be retroactively claimed.
