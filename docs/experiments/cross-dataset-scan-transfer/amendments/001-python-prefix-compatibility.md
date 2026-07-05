# Amendment 001: Python prefix compatibility

- Time: 2026-07-05 after execution of run-001
- Treatment data generated: yes
- Treatment results examined: no; analysis failed before constructing predictions
- Failure: system Python does not implement `str.removeprefix`
- Scope: replace `value.removeprefix("gpu-")` with an equivalent
  `startswith` plus slice
- Model, training/holdout allocation, metrics, thresholds, and exclusion rules changed: no
- Query rerun: no
- Replay input: immutable CPU/GPU journals and GPU event log from run-001
- Original analyzer: Git commit 8767ba48e and preregistration checksum
  `4d6112d5f875226163f3cd740b8e1268f993e6206742c06483448481d0b612d9`
