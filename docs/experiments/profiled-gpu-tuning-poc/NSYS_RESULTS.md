# Nsight Systems scan-stage profile

Profiled wall times are perturbed diagnostics. Kernel service is the sum of
kernel durations; busy time is the union of kernel intervals and therefore
does not double-count overlap.

| Partition MiB | Tasks | Spark stage ms | GPU ownership envelope ms | Kernel calls | Kernel service ms | Kernel busy ms | Busy/Spark stage | Max simultaneous kernels | Decode NVTX ms | Kernels overlapping decode windows ms | Memcpy MiB |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 128 | 87 | 569.0 | 557.1 | 10962 | 124.9 | 112.5 | 19.8% | 6 | 444.0 | 107.6 | 1682.1 |
| 2048 | 5 | 279.0 | 266.3 | 735 | 34.3 | 31.3 | 11.2% | 4 | 109.7 | 19.1 | 1682.3 |
| 16384 | 1 | 469.0 | 456.3 | 324 | 30.1 | 30.1 | 6.4% | 1 | 90.2 | 15.6 | 1686.3 |

GPU metric samples within each GPU-ownership envelope:

## 128 MiB

- DRAM Read Bandwidth [Throughput %]: mean 2.4, p50 1.0, p90 6, max 22 (557 samples).
- DRAM Write Bandwidth [Throughput %]: mean 2.1, p50 1.0, p90 5, max 12 (557 samples).
- PCIe RX Throughput [Throughput %]: mean 2.8, p50 1.0, p90 12, max 27 (557 samples).
- PCIe TX Throughput [Throughput %]: mean 1.1, p50 1.0, p90 1, max 3 (557 samples).
- SM Issue [Throughput %]: mean 2.1, p50 1.0, p90 5, max 11 (557 samples).
- SMs Active [Throughput %]: mean 10.8, p50 6.0, p90 36, max 72 (557 samples).

## 2048 MiB

- DRAM Read Bandwidth [Throughput %]: mean 4.7, p50 1.0, p90 10, max 84 (266 samples).
- DRAM Write Bandwidth [Throughput %]: mean 3.8, p50 1.0, p90 10, max 60 (266 samples).
- PCIe RX Throughput [Throughput %]: mean 5.1, p50 1.0, p90 27, max 40 (266 samples).
- PCIe TX Throughput [Throughput %]: mean 1.6, p50 1.0, p90 4, max 14 (266 samples).
- SM Issue [Throughput %]: mean 4.0, p50 0.0, p90 19, max 59 (266 samples).
- SMs Active [Throughput %]: mean 12.0, p50 0.0, p90 63, max 100 (266 samples).

## 16384 MiB

- DRAM Read Bandwidth [Throughput %]: mean 2.9, p50 0.0, p90 2, max 79 (456 samples).
- DRAM Write Bandwidth [Throughput %]: mean 2.2, p50 0.0, p90 2, max 47 (456 samples).
- PCIe RX Throughput [Throughput %]: mean 3.0, p50 0.0, p90 1, max 42 (456 samples).
- PCIe TX Throughput [Throughput %]: mean 1.4, p50 1.0, p90 1, max 17 (456 samples).
- SM Issue [Throughput %]: mean 2.4, p50 0.0, p90 3, max 52 (456 samples).
- SMs Active [Throughput %]: mean 7.1, p50 0.0, p90 22, max 100 (456 samples).
