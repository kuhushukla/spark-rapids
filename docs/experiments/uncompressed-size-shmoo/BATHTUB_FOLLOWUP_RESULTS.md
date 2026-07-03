# Dynamic-concurrency bathtub follow-up

## Validation

- Measured runs: 160 (80 partition, 50 batch, 30 layout).
- Stable result hashes within every cell: True.
- Source and sharded layout result hash match: True.
- Retry / split-retry / spill bytes: 0 / 0 / 0.

## Default variance

| Query | Repetitions | Mean ms | CV | Block-bootstrap median 95% interval | Approx. n=5 detectable effect |
|---|---:|---:|---:|---:|---:|
| common | 10 | 592.6 | 7.39% | 558.6–602.6 ms | 13.08% |
| variable_width | 10 | 740.5 | 5.28% | 711.2–753.2 ms | 9.35% |

Five paired blocks imply a minimum two-sided exact sign-flip p-value of 0.0625, even before Holm correction. Effect sizes and mechanism response are primary; the detectable-effect value is only a planning approximation.

## Partition mechanism sweep

| Query | Partition MiB | Median ms [block bootstrap 95%] | Stage/output/empty tasks | Max c | Output-task wave proxy | Last-wave proxy | Decoded output-task MiB | Max batch MiB | Max footprint MiB | Wait ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| common | 128 | 591.8 [558.6, 602.6] | 87/87/0 | 8.0 | 11.0 | 0.88 | 30.5 | 31.2 | 72.5 | 161.5 |
| common | 512 | 273.3 [263.6, 296.8] | 21/21/0 | 8.0 | 3.0 | 0.62 | 129.4 | 135.6 | 315.0 | 168.0 |
| common | 2048 | 255.9 [228.4, 312.9] | 5/5/0 | 4.0 | 2.0 | 0.25 | 534.3 | 542.8 | 1260.9 | 62.0 |
| common | 4096 | 283.9 [271.4, 309.8] | 3/3/0 | 3.0 | 1.0 | 1.00 | 1072.3 | 1023.9 | 2431.6 | 0.0 |
| common | 8192 | 389.1 [382.3, 393.3] | 2/2/0 | 1.0 | 2.0 | 1.00 | 1324.2 | 1023.8 | 3397.4 | 0.0 |
| common | 16384 | 447.5 [435.9, 480.6] | 1/1/0 | 1.0 | 1.0 | 1.00 | 2648.4 | 1023.8 | 3397.4 | 0.0 |
| common | 32768 | 451.2 [443.1, 457.4] | 1/1/0 | 1.0 | 1.0 | 1.00 | 2648.4 | 1023.8 | 3397.4 | 0.0 |
| variable_width | 128 | 731.8 [711.2, 753.2] | 87/87/0 | 8.0 | 11.0 | 0.88 | 46.7 | 47.7 | 110.3 | 181.0 |
| variable_width | 512 | 349.3 [336.0, 362.8] | 21/21/0 | 8.0 | 3.0 | 0.62 | 198.4 | 208.4 | 480.7 | 201.0 |
| variable_width | 2048 | 374.1 [349.2, 384.8] | 5/5/0 | 4.0 | 2.0 | 0.25 | 819.1 | 831.2 | 1920.9 | 133.0 |
| variable_width | 4096 | 383.9 [361.5, 474.9] | 3/3/0 | 3.0 | 1.0 | 1.00 | 1644.7 | 1023.8 | 2599.8 | 0.0 |
| variable_width | 8192 | 559.1 [549.0, 614.7] | 2/2/0 | 1.0 | 2.0 | 1.00 | 2030.5 | 1023.8 | 2600.1 | 0.0 |
| variable_width | 16384 | 677.4 [655.1, 799.8] | 1/1/0 | 1.0 | 1.0 | 1.00 | 4060.9 | 1023.8 | 2600.1 | 0.0 |
| variable_width | 32768 | 664.0 [656.1, 687.8] | 1/1/0 | 1.0 | 1.0 | 1.00 | 4060.9 | 1023.8 | 2600.1 | 0.0 |

Descriptive 5% regions:
- common: best 2048 MiB; within 5% [2048].
- variable_width: best 512 MiB; within 5% [512].

Paired block effects versus 128 MiB (negative is faster):

| Query | Partition MiB | Geometric mean change | Paired log-ratio 95% CI | Exact sign p | Holm p |
|---|---:|---:|---:|---:|---:|
| common | 512 | -53.1% | [-57.3%, -48.6%] | 0.0625 | 0.3750 |
| common | 2048 | -56.3% | [-61.8%, -50.0%] | 0.0625 | 0.3750 |
| common | 4096 | -51.5% | [-52.5%, -50.4%] | 0.0625 | 0.3750 |
| common | 8192 | -34.5% | [-37.8%, -31.1%] | 0.0625 | 0.3750 |
| common | 16384 | -23.8% | [-27.1%, -20.5%] | 0.0625 | 0.3750 |
| common | 32768 | -24.1% | [-28.3%, -19.6%] | 0.0625 | 0.3750 |
| variable_width | 512 | -52.9% | [-56.5%, -48.9%] | 0.0625 | 0.3750 |
| variable_width | 2048 | -49.9% | [-52.3%, -47.4%] | 0.0625 | 0.3750 |
| variable_width | 4096 | -46.5% | [-54.9%, -36.5%] | 0.0625 | 0.3750 |
| variable_width | 8192 | -22.8% | [-30.2%, -14.7%] | 0.0625 | 0.3750 |
| variable_width | 16384 | -6.1% | [-17.0%, 6.1%] | 0.2500 | 0.3750 |
| variable_width | 32768 | -9.9% | [-16.0%, -3.2%] | 0.0625 | 0.3750 |

## Cross-query bounded regret

| Partition MiB | Common regret | Variable-width regret | Worst regret | Minimum wave proxy | Maximum median footprint MiB |
|---:|---:|---:|---:|---:|---:|
| 128 | 131.3% | 109.5% | 131.3% | 11.0 | 110.3 |
| 512 | 6.8% | 0.0% | 6.8% | 3.0 | 480.7 |
| 2048 | 0.0% | 7.1% | 7.1% | 2.0 | 1920.9 |
| 4096 | 11.0% | 9.9% | 11.0% | 1.0 | 2599.8 |
| 8192 | 52.1% | 60.1% | 60.1% | 2.0 | 3397.4 |
| 16384 | 74.9% | 93.9% | 93.9% | 1.0 | 3397.4 |
| 32768 | 76.3% | 90.1% | 90.1% | 1.0 | 3397.4 |

The exploratory point-median minimax calculation gives 512 MiB 6.8% worst observed regret. Its 0.3-percentage-point advantage over 2,048 MiB is far below observed variation; 512 MiB is a conservative footprint/wave tie-break, not a uniquely identified optimum. This is a post-sweep diagnostic, not a validated policy.

## Batch-size sweep at fixed 4096-MiB partition treatment

| Query | Batch target MiB | Median ms [block bootstrap 95%] | Max batch MiB | Batches/task | Max c | Max footprint MiB |
|---|---:|---:|---:|---:|---:|---:|
| common | 256 | 391.0 [378.2, 431.5] | 256.0 | 4.0 | 3.0 | 1462.5 |
| common | 512 | 336.5 [316.7, 349.2] | 511.9 | 2.3 | 3.0 | 1754.2 |
| common | 1024 | 303.8 [299.0, 334.3] | 1023.9 | 1.7 | 3.0 | 2431.6 |
| common | 2048 | 294.9 [274.7, 319.5] | 1077.1 | 1.0 | 3.0 | 2502.1 |
| common | 4096 | 296.5 [285.6, 316.7] | 1077.1 | 1.0 | 3.0 | 2502.1 |
| variable_width | 256 | 521.9 [509.2, 575.9] | 255.9 | 5.7 | 3.0 | 2154.0 |
| variable_width | 512 | 435.1 [416.5, 506.2] | 511.9 | 3.3 | 3.0 | 2320.3 |
| variable_width | 1024 | 390.0 [371.1, 447.1] | 1023.8 | 1.7 | 3.0 | 2992.3 |
| variable_width | 2048 | 353.8 [332.1, 423.6] | 1650.3 | 1.0 | 3.0 | 3812.7 |
| variable_width | 4096 | 354.8 [334.7, 411.2] | 1650.3 | 1.0 | 3.0 | 3812.7 |

Paired block effects versus the 1024-MiB batch target (negative is faster):

| Query | Batch target MiB | Geometric mean change | Paired log-ratio 95% CI | Exact sign p | Holm p |
|---|---:|---:|---:|---:|---:|
| common | 256 | 26.8% | [22.7%, 31.1%] | 0.0625 | 0.2500 |
| common | 512 | 7.0% | [3.0%, 11.1%] | 0.0625 | 0.2500 |
| common | 2048 | -4.3% | [-7.8%, -0.7%] | 0.0625 | 0.2500 |
| common | 4096 | -4.3% | [-6.5%, -2.0%] | 0.0625 | 0.2500 |
| variable_width | 256 | 34.0% | [27.6%, 40.7%] | 0.0625 | 0.2500 |
| variable_width | 512 | 12.1% | [8.0%, 16.2%] | 0.0625 | 0.2500 |
| variable_width | 2048 | -8.9% | [-15.7%, -1.5%] | 0.0625 | 0.2500 |
| variable_width | 4096 | -7.7% | [-12.7%, -2.5%] | 0.0625 | 0.2500 |

## Physical-layout contrast

| Layout | Partition MiB | Median ms [block bootstrap 95%] | Planned/stage/output/empty tasks | Empty-task GPU hold ms | Max c | Max batch MiB |
|---|---:|---:|---:|---:|---:|---:|
| sharded | 128 | 540.0 [510.8, 603.7] | 87/87/87/0 | 0.0 | 8.0 | 31.2 |
| sharded | 2048 | 315.2 [292.8, 379.2] | 5/5/5/0 | 0.0 | 4.0 | 542.8 |
| sharded | 8192 | 417.1 [408.7, 599.1] | 2/2/2/0 | 0.0 | 1.0 | 1023.8 |
| source | 128 | 1576.8 [1323.3, 1669.1] | 46/46/12/34 | 3038.0 | 8.0 | 241.8 |
| source | 2048 | 559.7 [473.4, 586.3] | 3/3/3/0 | 0.0 | 3.0 | 920.1 |
| source | 8192 | 540.1 [525.8, 552.4] | 1/1/1/0 | 0.0 | 1.0 | 1017.6 |

## Interpretation

- Partition and batch sizing are coupled but distinct actuators. Once a task contains enough data, the batch target directly moves the emitted GPU boundary; partition sizing controls available task volume, task count, and batches/task.
- Dynamic admission is observable, but its task maximum is not a constant stage-wide concurrency and must not be substituted blindly into a wave equation.
- Physical file and row-group layout determines whether maxPartitionBytes can move actual task granularity.
- No retry/spill cliff was reached. The upper safety wall remains censored.
- These runs identify mechanisms and a candidate region; they do not validate a production bounded-box policy on untouched independent workloads.
