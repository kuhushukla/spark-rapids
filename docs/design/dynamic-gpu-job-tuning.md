# A Model-Driven Process for Dynamic GPU Job Tuning

## Purpose

This document defines a repeatable process for designing, validating, and operating dynamic tuning loops for GPU-accelerated Spark jobs. It is written for both engineers and the AI agents that help them investigate and implement changes.

The central idea is simple:

> Treat every tuning rule as a falsifiable controller over a real decision boundary, not as a collection of plausible configuration values.

A useful controller has an objective, measurements, a model, an actuator that can take effect at the required time, uncertainty bounds, safety constraints, and a test that can prove the idea wrong. The model can be a heuristic, an analytical equation, or a learned policy. Complexity is earned only when a simpler model fails.

This guide consolidates and corrects the two research reports and two architecture presentations that motivated it. The source material contains strong ideas, but it sometimes presents proposals as current capabilities, treats diagnostic models as controllers, conflates input partitions with GPU batches, and makes equations stronger than the available evidence supports. The corrected distinctions below are essential to building something that works.

## Executive summary

Use this sequence:

1. Define the objective and hard safety constraints.
2. Trace the dataflow and its critical path.
3. Inventory only control surfaces that actually exist, including where and when each value is read.
4. Decompose the problem by decision boundary: task, stage/query, cross-run, and routing/fleet.
5. Create a model card with units, assumptions, uncertainty, validity, and a falsification test.
6. Establish a reproducible baseline and collect raw evidence.
7. Validate offline, then in shadow mode, then with guarded canaries.
8. Compose controllers through explicit precedence and ownership rather than letting independent tuners fight.
9. Persist history only with enough identity and freshness metadata to know when it is reusable.
10. Promote a controller only when it improves the declared objective across its validity region without violating correctness or safety.

The most important present-day example is already in this repository. Dynamic GPU task admission is not merely theoretical: [`GpuSemaphore.scala`](../../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuSemaphore.scala) estimates per-stage task memory and admits tasks through memory-weighted permits. It is the worked example in this guide.

## 1. Use precise language about evidence

Every design document, experiment, recommendation, and agent report must label important claims with one of these states:

| State | Meaning | Required evidence |
|---|---|---|
| **Implemented** | Present in the repository or a named dependency | Symbol, read path, tests, version, and effective lifecycle |
| **Measured** | Observed in a reproducible experiment | Raw results, manifest, and analysis; repetitions and variance for performance/statistical claims. A deterministic instrumentation check may use one mechanically validated run if labeled as such. |
| **Externally validated** | Supported by authoritative prior work | Primary paper or official documentation, plus limits of analogy |
| **Hypothesis** | A falsifiable explanation or model | Assumptions, predicted outcome, and falsification experiment |
| **Proposal** | A possible architecture or actuator not currently available | Required integration work, authority, and validation plan |

Do not use “real,” “dynamic,” “runtime,” or “supported” without stating the scope and version. For example, a setting can be mutable in `SQLConf` while still being ineffective for an already-planned scan because the consumer captured it earlier.

### 1.1 Corrections to the source material

The following corrections affect the architecture, not just wording:

- Per-stage dynamic GPU admission already exists in this plugin. It must be studied as an implemented controller, not proposed as greenfield work.
- The p80 choice in that controller is an implementation policy. SQL Server Memory Grant Feedback is useful precedent for history and high-percentile feedback, but it does not prove that p80 is optimal for GPU memory.
- Roofline and Ridgeline identify possible binding resource ceilings. They do not supply a controller setpoint or prove that an end-to-end Spark stage can reach the ceiling.
- Increasing operations alone never improves runtime. Extra compute is useful only when it reduces transferred data, avoids other work, or enables beneficial overlap.
- Reducing shuffle partition count does not normally reduce shuffle bytes. It changes task size, launch/scheduling overhead, and potentially spill behavior.
- Compute, memory, network, storage, codec, and coordination demands do not share one universal “operational intensity.” Model each lane using the bytes, records, operations, and passes that actually traverse it.
- Stage time is not always the maximum of compute, memory, and network time, nor the sum of every component. `max` is an optimistic lower bound only when work overlaps; a sum applies only to serial work. The schedule and critical path decide.
- Spark stages form a DAG. Shuffle dependencies impose barriers, narrow operators pipeline within a stage, and independent branches may overlap. Predict the critical path under contention, not the sum of all stage durations.
- File partition size and GPU batch size are different. One task can consume or produce multiple batches. Model both distributions.
- A push/pipelined exchange is not ordinary Spark pull-shuffle behavior. It is an execution-engine proposal with new backpressure, failure recovery, scheduling, and materialization semantics.
- cuCascade, Theseus, partial-partial aggregation, and similar systems are valuable external research, not current Spark RAPIDS control surfaces.
- “No dominant bottleneck” is not an optimization goal. Every execution has a binding constraint. The goal is best latency, throughput, or cost subject to correctness and safety.

## 2. Start with an optimization contract

Do not begin by selecting knobs. Write the contract first.

### 2.1 Objective

Choose one primary objective and declare secondary objectives. Examples:

- minimize p50 job completion time;
- minimize p95 stage completion time;
- maximize steady-state rows/s;
- minimize dollars per successful query;
- maximize completed work per GPU-hour.

GPU utilization, SM occupancy, HBM utilization, and network saturation are diagnostic signals, not objectives. A query that finishes sooner can have lower average utilization.

For repeated observations $j$, a constrained objective can be written as:

$$
\min_\pi\; Q_p\!\left(\{T_j(\pi)\}_j\right)
$$

subject to:

$$
P(\text{fatal OOM}\mid\pi) \le \epsilon,\qquad
Q_{p_s}(S_j(\pi)) \le S_{\max},\qquad
\text{correctness}(\pi)=\text{true}
$$

where $\pi$ is the policy, $T$ is time, $S$ is spill or another safety signal, and $Q_p$ is a declared percentile. The thresholds and percentiles must come from product requirements or measurements, not from convention.

### 2.2 Hard constraints

At minimum, state:

- semantic equivalence and accepted numerical differences;
- fatal OOM and executor-loss policy;
- maximum retry, spill, or recomputation budget;
- resource/cost ceiling;
- fairness and multi-tenant requirements;
- maximum tuning overhead;
- permitted actuator scope;
- rollback and kill-switch behavior.

### 2.3 Workload identity

A result is reusable only within a documented identity. Useful keys include:

- normalized logical and physical plan fingerprints;
- table snapshot/version and file layout;
- schema, projected columns, predicates, and statistics version;
- input logical bytes, encoded bytes, row count, null density, cardinality, and skew;
- Spark, RAPIDS, cuDF, CUDA, JVM, Scala, and platform versions;
- GPU, CPU, memory, disk, network, executor, and topology details;
- relevant configuration and controller/model version.

Do not normalize away literals blindly. Predicate literals can change selectivity by orders of magnitude. Reuse requires either selectivity features or a confidence penalty.

## 3. Model work as demands, rates, and a schedule

### 3.1 Per-lane bounds and calibrated estimates

For resource lane $i$, define demand $D_i$. A defensible upper bound on attainable aggregate rate, $\bar R_i$, gives a time lower bound:

$$
t_i \ge \frac{D_i}{\bar R_i}
$$

A typical measured sustainable rate is not a capacity ceiling. It produces a calibrated time estimate, which must carry a prediction interval:

$$
\hat t_i = \frac{D_i}{R_i^{\text{typical}}}
$$

Examples:

- GPU compute: operations or records divided by sustained operator throughput;
- HBM: bytes actually transferred divided by sustained HBM bandwidth;
- host-device: transferred bytes divided by sustained PCIe/NVLink bandwidth;
- network: wire bytes divided by effective per-flow or aggregate bandwidth;
- disk: bytes read or written divided by sustainable storage throughput;
- codec: uncompressed input bytes divided by compression/decompression throughput;
- coordination: a fitted scheduling or synchronization term.

Use empirical rates from the actual topology and concurrency regime for calibrated estimates. A hard or otherwise defensible capacity ceiling supports a deterministic lower bound under its stated assumptions. A statistical upper confidence bound on rate supports only a probabilistic time lower bound at the declared coverage/confidence. Marketing peaks can bound hardware capability but are not expected rates.

The familiar Roofline bound for a floating-point kernel is:

$$
P \le \min(P_{\text{compute}}, I_A B_{\text{memory}})
$$

where $I_A$ is operations per memory byte. The original [Roofline model](https://www.osti.gov/biblio/1407078) is a diagnostic upper-bound model. SQL operators may require integer-operation, instruction, record-throughput, or time-based variants because FLOPs are often not representative.

The [Ridgeline model](https://arxiv.org/abs/2209.01368) adds network intensity to classify compute, memory, and communication limits in distributed workloads. It is useful for generating hypotheses. It does not encode Spark barriers, task granularity, spill, failures, scheduling, or controller latency.

[Hierarchical Roofline analysis](https://arxiv.org/abs/2009.05257) uses measured ceilings at multiple memory levels to localize a kernel's limiting level. Nsight Compute can supply the kernel evidence, but this remains a diagnostic sub-model: it neither predicts a Spark stage schedule nor supplies a controller action by itself. Use it when an operator-level GPU lane dominates the stage model, then connect the kernel finding back to the end-to-end critical path.

### 3.2 Composition requires a schedule

If two components are serial:

$$
T = t_1 + t_2
$$

If they start together and demonstrably overlap completely, with coordination accounted separately:

$$
T = \max(t_1,t_2)+t_{\text{coordination}}
$$

If overlap is possible but not established, $\max(t_1,t_2)$ is only a lower bound.

For partial overlap:

$$
T = t_1+t_2-t_{\text{overlap}}+t_{\text{coordination}}
$$

The overlap term must be measured or bounded. Never count the same byte transfer twice, and never hide a materialization or spill-read phase merely because a diagram shows pipelining.

Apply the rule at the level where the scheduling actually occurs:

- **Inside one task:** iterator/operators may be serial, fused, or pipelined. Add serial phases. Use a pipeline model only when buffers and execution traces show concurrent producer/consumer work, and include fill, drain, and backpressure.
- **Across tasks in one stage:** tasks are parallel, but not independent when they share an executor GPU, RMM pool, CPU cores, disk, or network. Model task waves or an explicit queue. Do not sum task durations; do not multiply single-task throughput by concurrency unless measured aggregate throughput scales that way.
- **At a stage barrier:** tasks within the map stage run in parallel, and the stage completes at its last successful required task. Under ordinary Spark shuffle, the dependent reduce stage begins only after the map-output barrier is satisfied. Map and reduce stage durations are therefore serial along that dependency even though each stage contains parallel work.
- **Across independent DAG branches:** branches can overlap when Spark schedules them concurrently, but shared executors and I/O reduce their rates. Use the completion time of the resource-constrained parallel schedule.
- **Across resource lanes:** compute, copy, network, storage, and codec phases overlap only when the concrete implementation issues them concurrently and has enough buffering. Otherwise they are serial.

For $n$ equal tasks of duration $t$ on $c$ truly equivalent parallel slots, the ideal stage model is:

$$
T_{\text{stage, ideal}}=\left\lceil\frac{n}{c}\right\rceil t
$$

This formula is deliberately narrow. With skew, heterogeneous executors, failures, or shared-GPU interference, use the actual/list-scheduled task durations and model the barrier as the last required completion. Two useful lower bounds are:

$$
T_{\text{stage}}\ge \max\left(\max_j t_j,\frac{D_{\text{stage}}}{\bar R_{\text{aggregate}}}\right)
$$

Here $\bar R_{\text{aggregate}}$ must be a defensible upper bound on attainable aggregate rate. If only a typical measured rate is available, the quotient is a calibrated estimate with uncertainty, not a lower bound. Neither lower bound is a prediction by itself. Queueing, waves, resource contention, retries, and stragglers supply the gap.

For a Spark stage DAG $G=(V,E)$, a dependency-only lower bound is the longest stage path:

$$
T_{\text{job}} \ge \max_{p\in\text{paths}(G)} \sum_{v\in p} T_v
$$

A runtime prediction requires a resource-constrained schedule: independent paths may contend for the same executors, GPU, disk, or network, while other branches may overlap. A useful model therefore contains both dependency edges and shared-resource constraints.

### 3.3 Coordination is fitted, not wished away

An empirical term such as

$$
T(N)=T_{\text{work}}(N)+a+bN
$$

can describe startup and per-worker coordination over a measured range. It is not automatically the Universal Scalability Law. If USL is used, fit its contention and coherency terms explicitly and validate out of sample.

Similarly, the claim that cost becomes independent of node count is true only under ideal inverse scaling with no skew, fill/drain, coordination, task-granularity, or bandwidth penalties. Present it as a counterfactual lower bound, not a forecast.

## 4. Find the real decision boundary

An actuator is useful only if it can take effect before the remaining work is committed.

| Plane | Earliest useful observation | Typical decision | Current authority |
|---|---|---|---|
| Task/executor | allocation, retry, task progress | GPU admission, local batch splitting, spill/retry | RAPIDS executor code |
| Query/stage | completed map statistics, plan statistics | AQE coalescing/skew/join choice | Spark AQE and plugin rules |
| Cross-run | completed job history | initial configs, cluster sizing, candidate routing | launcher, service, or offline tuner |
| Router/fleet | queue, SLO, price, available hardware | engine/cluster/hardware choice | external control plane |

These are three adaptation timescales plus a separate decision plane. A router does not merely run a slower executor loop; it has different authority, risks, and rollback semantics.

### 4.1 Control-surface inventory

Before designing a loop, trace each candidate from declaration to consumption:

1. configuration/symbol definition;
2. user-visible contract and default;
3. every read site;
4. whether the value is read at startup, planning, stage creation, task creation, or repeatedly;
5. cached/lazy copies;
6. driver-to-executor propagation;
7. Spark-version and Databricks shims;
8. metrics and tests proving the effect;
9. interactions with resource scheduling and other caps.

Representative current surfaces are:

| Surface | Lifecycle in this repository | What it actually controls |
|---|---|---|
| `spark.rapids.sql.concurrentGpuTasks` | runtime initial estimate, read per stage/job context | starting memory estimate; dynamic admission is implemented by the semaphore |
| `spark.rapids.sql.concurrentGpuTasks.dynamic` | runtime, captured in a stage estimator | enables measured per-stage task memory estimates |
| `spark.rapids.sql.maxConcurrentGpuTasks` | internal; semaphore is initialized once at executor startup | a fixed upper bound even though the entry is not marked startup-only |
| `spark.rapids.sql.batchSizeBytes` | runtime config, read by many operators/readers | target GPU batch bytes, not file partition bytes |
| `spark.sql.files.maxPartitionBytes` | SQL planning/runtime config | file partition planning; changing it after the scan is planned does not resize existing partitions |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | AQE runtime SQL config | post-shuffle coalescing/skew target; behavior also depends on AQE rules such as `parallelismFirst` |
| `spark.rapids.memory.pinnedPool.size` | startup-only | executor pinned-memory pool |
| `spark.rapids.shuffle.compression.codec` | internal and startup-only | RAPIDS shuffle codec, not a per-shuffle actuator |
| `spark.shuffle.compress` | Spark application/shuffle behavior | map-output compression; do not assume safe per-dependency mutation without tracing the shuffle implementation |

The RAPIDS lifecycle labels are defined in [`RapidsConf.scala`](../../sql-plugin/src/main/scala/com/nvidia/spark/rapids/RapidsConf.scala) and rendered in [`docs/configs.md`](../configs.md) and [`advanced_configs.md`](../additional-functionality/advanced_configs.md). Spark documents AQE and runtime SQL configuration in its [performance tuning](https://spark.apache.org/docs/latest/sql-performance-tuning) and [configuration](https://spark.apache.org/docs/latest/configuration) guides.

### 4.2 Feasibility classes

Place every proposed action in one class:

- **Available now:** a repeated read or existing adaptive mechanism can affect remaining work.
- **Available next boundary:** can apply at the next stage, query, or job.
- **Requires plugin/Spark extension:** telemetry exists, but no actuator is connected at the necessary boundary.
- **Requires another execution architecture:** changes shuffle/materialization/fault-tolerance semantics.

This classification prevents an agent from “implementing” a controller that only mutates an ignored configuration value.

## 5. The controller contract

Every controller needs a model card containing the following fields.

| Field | Required content |
|---|---|
| Objective | Primary metric, aggregation, and target population |
| Safety constraints | Correctness, OOM, spill, retry, cost, fairness |
| State/features | Available at decision time; units and provenance |
| Estimator/model | Equation or algorithm, parameters, prior, uncertainty |
| Actuator | Exact symbol/API, scope, lifecycle, bounds |
| Decision boundary | When observations arrive and when action can take effect |
| Policy | Decision rule, hysteresis, step limit, cooldown |
| Learning signal | Post-action measurements and attribution window |
| Validity region | Workloads, versions, hardware, and ranges covered |
| Invalidation | Drift, plan/schema/layout/version changes, low confidence |
| Fallback | Safe behavior when data is missing or model is invalid |
| Falsification | Experiment and result that would reject the model |
| Version | Controller, feature schema, and model/data versions |

### 5.1 State is not measurement

Distinguish:

- **raw measurements:** allocation events, bytes, durations, rows, retry counts;
- **derived features:** expansion ratio, skew coefficient, estimated memory;
- **hidden state:** true simultaneous peak demand or future selectivity;
- **control action:** permit demand, partition target, plan choice;
- **outcome:** latency, throughput, spill, OOM, cost.

Do not train on a feature unavailable at decision time. Do not treat an action-dependent measurement as an independent workload property without recording the action.

### 5.2 Uncertainty changes the action

Return a distribution or interval, not only a point:

$$
\hat y(x) \pm q_{1-\alpha}(x)
$$

A safety-sensitive controller should use an upper confidence bound on memory and a lower confidence bound on benefit. Low confidence can cause the controller to hold the current action, choose a conservative prior, or remain in shadow mode.

Percentiles are policy choices. Sample count, interpolation method, priors, censoring, retries, skew, and recency all matter. A percentile copied from another database system is a hypothesis, not validation.

### 5.3 Stability and delayed feedback

At minimum consider:

- observation delay and actuation delay;
- cold-start and warm-up bias;
- hysteresis around decision thresholds;
- maximum change per action;
- minimum dwell/cooldown time;
- saturation and hard bounds;
- integral wind-up or accumulated error, if relevant;
- skew and regime changes;
- retries/speculation duplicating observations;
- controllers changing each other’s plant.

Full PID control is rarely the right default for discrete, delayed, stage-boundary actions. Begin with bounded rules, hysteresis, or model-predictive candidate selection. Introduce more control machinery only after system identification shows it is needed.

## 6. Worked implementation: dynamic GPU task admission

### 6.1 What exists

The current implementation is executor-local and stage-specific:

1. [`GpuTaskMemoryEstimator`](../../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuSemaphore.scala) updates asymmetrically over a 100 ms active-time window: a measured maximum above the prior is adopted immediately, while a lower maximum is blended downward; blocked/lost time is excluded.
2. `GpuStageMemoryEstimator` stores up to 200 completed values and combines them with active estimates.
3. `StatEstimator` pads to at least four values using the prior and computes p80.
4. `GpuSemaphore` converts the estimate into permits of approximately 32 MiB each.
5. `PrioritySemaphore` admits tasks while respecting the permit budget derived from the configured RMM pool allocation and an optional maximum task limit.
6. Task completion feeds `RmmSpark.getMaxGpuTaskMemory` back into the stage estimator.
7. `spark.rapids.sql.concurrentGpuTasks.dynamic` enables the adaptive estimate by default; `spark.rapids.sql.concurrentGpuTasks` influences the initial estimate.

This is not implemented by repeatedly changing the Spark configuration. The configuration initializes a local controller; admission changes because each task requests permits based on the current stage estimate.

### 6.2 Reconstructed model card

**Objective:** admit enough GPU work for throughput while applying memory-weighted admission pressure. The current permit arithmetic is an approximate budget, not a strict bound on aggregate estimated demand.

**State:** stage ID, task attempt ID, RMM pool allocation, initial batch/concurrency configuration, completed per-task maximums, live maximums, and blocked/lost time.

**Estimator:**

$$
\hat M_s=P_{80}(M_{s,\text{completed}}\cup \hat M_{s,\text{active}}\cup M_{\text{prior padding}})
$$

The live estimate jumps immediately when the observed maximum exceeds the prior and blends only downward over active time. With four or fewer combined completed/active observations after prior padding, this percentile implementation's p80 is the maximum of the combined values.

**Actuator:** task admission through memory permits:

$$
p_s=\max\left(1,\left\lfloor\frac{\hat M_s}{32\text{ MiB}}\right\rfloor\right)
$$

bounded by RMM-pool-derived permits and the optional maximum concurrent-task limit. This is not necessarily the GPU's advertised physical VRAM.

**Boundary:** every semaphore acquisition can use the latest stage estimate; completed tasks update history.

**Fallback/prior:** configured RMM pool allocation divided by configured or inferred initial concurrency. When concurrency is not configured, the initial inference uses target batch size and is capped at four.

### 6.3 What the model does not prove

The sum of individual task maxima is not the same as the executor’s simultaneous peak unless peaks coincide. Per-task attribution may omit shared reservations, allocator pool behavior, asynchronous allocations, and other executor activity. Memory permits are therefore an admission proxy, not a physical conservation proof.

Permit conversion uses floor division in 32 MiB units and caps one request at the total permit count. Each request can under-account by less than one permit, multiple admitted requests can accumulate quantization slack, and an estimate larger than the RMM pool is capped rather than rejected. The policy therefore does not strictly bound the sum of task estimates. Waiting requests are priority ordered; a large high-priority request can also block smaller requests behind it, so wait behavior is not determined by free permits alone.

An active-task estimate jumps immediately above its prior but blends downward from that prior over 100 ms of active time; it is not a prediction of the task’s future peak. A task whose peak occurs later can temporarily lower the stage estimate before that peak is observed. The controller also does not prove that p80 maximizes throughput, that four prior-padded samples converge quickly enough, or that one estimate represents a skewed stage. Stage estimators are executor-local, retain at most 100 stage IDs, and a retried stage ID can reuse retained state. These are testable design choices.

### 6.4 Validation plan

Validate at least these properties:

- **Estimator accuracy:** compare admitted aggregate predicted demand with executor/device watermarks at aligned timestamps.
- **Attribution:** reconcile task-attributed memory, shared memory, RMM pool/reservation metrics, and unclassified residuals.
- **Cold start:** measure early-stage oversubscription and undersubscription under different priors.
- **Skew:** interleave small and large partitions and test ordering sensitivity.
- **Temporal overlap and late peaks:** create workloads whose task peaks align, do not align, and occur after the 100 ms live-estimate ramp.
- **Quantization/capping:** test estimates just below and above 32 MiB boundaries, accumulated slack at high concurrency, and estimates larger than the RMM pool.
- **Stability and recency:** inspect permits, wait time, throughput, retries, spills, stage-ID reuse/LRU behavior, and oscillation over task order.
- **Safety:** inject retryable and split-and-retry OOM paths, including capped requests; verify fallback plus no leak or deadlock.
- **Performance:** compare dynamic-on, dynamic-off fixed candidates, and an oracle chosen after the run.
- **Scope:** repeat across operator families, formats, schemas, GPU sizes, and Spark shims.

Metrics must distinguish semaphore wait, GPU work, GPU bubble, retry, spill-to-host, spill-to-disk, and read-back. Current “GPU bubble” time is a scheduling proxy based on whether threads are waiting for the semaphore; it is not NVML SM utilization or proof that the device is idle. Several task memory and retry metrics are finalized only on semaphore release or task completion, so event-log accumulators are audit/cross-run sensors rather than a low-latency control bus. A zero fatal-OOM count can hide a controller that performs excessive retry or spill.

A [versioned instrumentation smoke test](../experiments/dynamic-gpu-admission/README.md) is the first local **Measured instrumentation** evidence: with the same `concurrentGpuTasks=2` initial-estimate input in both modes, the dynamic stage-0 maximum was above two (eight in the committed run), while every recorded static update was two and its maximum was two. These updates are recorded at task end and are not an acquisition-order timeline. The complete canonical result hash matched, required GPU plan nodes were present, and no task attempt failed. Its single unrandomized run validates this actuator response, not a latency or throughput benefit.

### 6.5 Candidate refinements are hypotheses

Possible refinements include operator/plan features, input-size regression, an upper confidence bound, separate skew regimes, cross-run priors, and explicit reserve accounting. Recursive least squares is one candidate only if residuals demonstrate a stable, approximately linear relationship that must adapt online; it is not the default. Each refinement adds state and failure modes. Adopt one only when a controlled experiment demonstrates a material failure of the existing estimator and the refinement improves hold-out results.

## 7. Candidate sub-models

### 7.1 Input partition and GPU batch sizing

Define the volumes explicitly:

- $C$: encoded/compressed input bytes assigned to a file partition;
- $U$: decoded bytes for all encoded columns;
- $s_c$: projected-column fraction by decoded bytes, conditional on the same encoded payload used for $C/U$;
- $s_r$: conditional row-group/page survival after metadata pruning;
- $s_f$: conditional row survival after filters applied before the modeled resident point;
- $e$: type/layout/intermediate expansion after decode;
- $A$: downstream operator memory amplification;
- $M_b$: per-task memory budget at the modeled phase.

If $r=C/U$, where $r>0$ and $C$ and $U$ cover the same encoded/decoded payload, then a first-order resident-memory model is:

$$
M_{\text{resident}} \approx \frac{C}{r}s_c s_r s_f e A + \beta
$$

and the encoded partition target satisfying $M_{\text{resident}}\le\phi M_b$ is:

$$
C_{\text{target}} \le
\frac{(\phi M_b-\beta)r}{s_c s_r s_f e A}
$$

`REPORT_2.md` incorrectly divided by $r$. With $r=C/U$, stronger compression means a smaller $r$, so the same encoded bytes expand more and the safe encoded target becomes smaller.

Memory safety is only the upper bound. A scan also has a possible lower efficiency bound:

$$
B_{\min,\text{efficient}} \le B_{\text{batch or partition}} \le B_{\max,\text{safe}}
$$

The lower bound is a **Hypothesis**, not `REPORT_1.md`'s unvalidated SM-count formula. Falsify it with controlled encoded-partition and decoded-batch sweeps at fixed query/data identity: measure rows/s, kernel-launch overhead, GPU work/bubble, admission, peak memory, retry, and spill. Reject a general lower-bound rule if no stable throughput knee appears across hold-out schemas/operators or if another feature explains it. Keep partition and batch knees separate because a task can emit multiple batches.

This remains a hypothesis because decode can be chunked, buffers have overlapping lifetimes, filters execute at different points, and one task can produce several GPU batches. The repository already implements chunked Parquet and ORC reading with a soft memory limit; that fact narrows the model but does not establish Unified Virtual Memory behavior or validate a particular partition target. Collect separate distributions for:

- encoded bytes per file partition;
- decoded/projected bytes per task;
- batches per task;
- rows and bytes per batch;
- per-batch and per-task peak memory;
- downstream amplification and output bytes.

Do not derive a per-task target and divide it by concurrency again. Concurrency is already represented in the per-task memory budget.

### 7.2 Shuffle partition sizing with AQE

A first-order count is:

$$
N=\left\lceil\frac{B_{\text{shuffle}}}{B_{\text{target}}}\right\rceil
$$

but $B_{\text{target}}$ must satisfy a range, not one magic value:

$$
B_{\min,\text{efficient}} \le B_{\text{target}} \le
\frac{M_{\text{task, safe}}-\beta}{A_{\text{downstream}}}
$$

The lower bound amortizes scheduling, fetch, serialization, and kernel-launch overhead. The upper bound protects downstream memory. Skew may require split partitions even when the mean is safe.

Spark AQE already uses runtime map-output statistics to coalesce partitions, split skewed partitions, and reconsider some join strategies. A GPU-aware target is feasible only after tracing the exact AQE rule, configuration snapshot, downstream operator, and shim behavior. `parallelismFirst=true` can cause AQE to ignore the advisory target during coalescing, so setting the target alone is not an implementation.

Changing partition count adjusts task sizing and overhead; it does not by itself reduce shuffle data volume. Reduction requires pruning, aggregation, a different plan, or compression.

### 7.3 Join strategy and Bloom filters

Represent each alternative as a resource-constrained execution graph:

$$
T_{\text{BHJ}}=\operatorname{makespan}(G_{\text{BHJ}},\mathcal R)
$$

$$
T_{\text{SHJ}}=\operatorname{makespan}(G_{\text{SHJ}},\mathcal R)
$$

where $\mathcal R$ contains executor, CPU, GPU, memory, disk, and network capacities/queues. Add elapsed phase makespans only across proven dependency-serial edges. The two shuffled parents can execute in parallel; broadcast collection/transfer fans out across executors; build precedes probe within a join task; fetch, build, and probe behavior depends on the concrete implementation. Model task waves, shared-resource contention, and the last required finisher rather than summing aggregate work. Include executor count, wire bytes, GPU-resident hash-table amplification, repeated builds, overlap, skew, timeout/fallback, and concurrent-query memory pressure. Spark statistics and AQE already provide some cardinality and runtime size information; history augments missing or stale statistics rather than replacing them.

A Bloom filter is useful when expected avoided downstream cost exceeds build, broadcast, probe, and false-positive cost:

$$
E[C_{\text{avoided}}] > C_{\text{build}}+C_{\text{distribute}}+C_{\text{probe}}+E[C_{\text{false positive}}]
$$

Plan injection also requires semantic-equivalence tests, a legal optimizer boundary, freshness, and rollback. It is not an executor-local action.

### 7.4 Compression

For uncompressed bytes $D$, compressed ratio $r=D_c/D$, compression rate $R_c$, decompression rate $R_d$, and transfer bandwidth $B$, a serial first-order comparison is:

$$
T_{\text{plain}}=\frac{D}{B}
$$

$$
T_{\text{compressed}}=\frac{D}{R_c}+\frac{rD}{B}+\frac{D}{R_d}
$$

If phases overlap, replace the sum with the measured pipeline schedule. Include serialization, copies, disk, network, chunking, and metadata. Compression is not “almost always” beneficial: fast links, incompressible data, small blocks, scarce CPU/GPU capacity, or poor overlap can make it slower.

For CPU compression and decompression that are serial with transfer, define expansion ratio $q=D/D_c>1$, $n$ effective codec cores, and measured per-core rates $C_c$ and $C_d$. The candidate beats plain transfer only if:

$$
\frac{D}{nC_c}+\frac{D}{qB}+\frac{D}{nC_d}<\frac{D}{B}
$$

which implies:

$$
n>\frac{Bq}{q-1}\left(\frac{1}{C_c}+\frac{1}{C_d}\right)
$$

This assumes linear codec scaling and serial phases. If compression, transfer, and decompression overlap, model the concrete pipeline, finite buffers, fill/drain, and shared-core contention instead.

The current RAPIDS shuffle codec configuration is internal and startup-only. Per-shuffle selection requires a new contract and implementation; cross-run application selection is a nearer-term experiment.

### 7.5 Spill and tiered movement

Model each tier and direction separately. For a fully serial spill-and-read-back path:

$$
T_{\text{spill episode, serial}} = T_{D\rightarrow H}+T_{H\rightarrow S}+T_{S\rightarrow H}+T_{H\rightarrow D}+T_{\text{coordination}}
$$

For chunked or asynchronous paths, derive the episode from the resource-constrained pipeline schedule, including fill, drain, buffers, and backpressure. Terms occur only for paths actually taken and may overlap across buffers. Host spill and disk spill are not one PCIe operation. “Spill bytes” without tier, direction, compression, residency, and reuse is insufficient telemetry.

Pinned pool size is startup-only, so current-job spill data can recommend the next application’s pool but cannot resize the existing pool. External tiered-memory systems are architecture references, not implicit plugin features.

### 7.6 Blocking versus pipelined exchange

Spark’s ordinary shuffle provides materialization and retry boundaries. A streaming/push exchange can reduce barriers and overlap producer, network, and consumer work, but it changes:

- fetch/push scheduling;
- buffer ownership and backpressure;
- consumer availability;
- failure recovery and recomputation;
- executor loss semantics;
- skew behavior;
- spill/materialization points;
- observability and accounting.

Evaluate it as a separate execution architecture with a concrete dataflow. Do not model it as toggling a Spark configuration. Theseus is relevant external evidence that accelerator-native engines can exploit specialized asynchronous data movement, but its [published results](https://arxiv.org/abs/2508.05029) do not validate the same change inside Spark.

Use the [`gpu-tuning-model-design`](../../skills/gpu-tuning-model-design/SKILL.md) skill to compare baseline and proposed execution graphs before prioritizing pipelining, background shuffle, MPP-style processing, or another new architecture.

## 8. Experiment protocol

### 8.1 Reproducibility manifest

Capture before the run:

```yaml
experiment_id: stable-human-readable-id
hypothesis: one falsifiable sentence
controller_version: source revision or disabled
workload:
  query: immutable source or hash
  data_snapshot: immutable identifier
  seed: value or not-applicable
  scale: logical rows/bytes and encoded bytes
software:
  spark: exact version/distribution
  rapids: git SHA and build profile
  cudf_cuda_driver_jvm: exact versions
hardware:
  gpu_cpu_memory_disk_network: exact topology
configuration:
  baseline: complete effective configuration
  treatment: one declared delta
procedure:
  warmups: count and cache policy
  repetitions: count
  ordering: randomized_or_declared
  timeout_and_failure_policy: declared
artifacts:
  raw_logs_metrics_plans: immutable paths
```

Use GB/GiB and logical/encoded/resident/wire/spilled bytes consistently.

### 8.2 Experimental stages

1. **Measurement validation:** verify counters reconcile and clocks/scopes align.
2. **Micro/model isolation:** vary one input while holding confounders fixed.
3. **Fit:** estimate parameters using training cases.
4. **Hold-out:** validate workloads, scales, skew, or hardware not used to fit.
5. **Counterfactual replay:** compare candidate actions when safe and possible.
6. **Shadow:** emit decisions without applying them.
7. **Canary:** bounded application on a small population.
8. **Promotion:** expand only after predeclared success criteria pass.

Three repetitions are a bare minimum for a smoke comparison, not a universal standard. Choose sample size from observed variance and the effect size worth detecting. Report individual runs and confidence intervals, not only means.

### 8.3 Baselines and oracles

Compare against:

- the existing default/controller;
- representative fixed settings;
- current documented recommendations or Auto-Tuner output;
- an offline oracle selected after exhaustive candidates for the test case;
- ablations that remove each new feature.

NVIDIA’s [RAPIDS Auto-Tuner](https://docs.nvidia.com/spark-rapids/user-guide/latest/partials/tools-autotuner.html) and Spark AQE are baselines and integration context, not evidence that a new policy works.

### 8.4 Required outcome dimensions

Measure:

- correctness and plan shape;
- end-to-end and critical-path time;
- task/stage distributions and stragglers;
- throughput and cost;
- GPU work, bubble, and semaphore wait;
- memory watermark and attribution residual;
- retry and split-and-retry counts/time;
- spill bytes/time by tier and direction;
- shuffle logical/wire bytes, fetch/write time, and skew;
- CPU, disk, network, PCIe/NVLink, and codec utilization;
- tuning overhead and decision latency.

### 8.5 Failure-oriented cases

Include skew, empty/tiny inputs, highly compressible and incompressible data, wide strings/nested types, null-heavy data, stale statistics, stage retries, speculative attempts, executor loss, dynamic allocation, concurrent jobs, cold caches, and topology heterogeneity where supported.

## 9. Safe rollout

### 9.1 Shadow mode

The controller records:

- observation and feature values;
- candidate action and confidence;
- action the system actually used;
- predicted outcome and safety margin;
- realized outcome and attribution window;
- reasons for abstention.

Shadow mode validates decision timing and counterfactual coverage without changing execution.

### 9.2 Canary controls

Require:

- feature flag/kill switch;
- hard actuator min/max;
- maximum step size;
- cooldown/hysteresis;
- timeout and conservative fallback;
- per-workload or per-tenant eligibility;
- controller/model version in metrics;
- automatic disable on invariant violation;
- preservation of enough evidence for replay.

### 9.3 Promotion criteria

Predeclare thresholds. A typical decision might require:

- no correctness regressions;
- no increase in fatal failures;
- retry/spill safety budgets satisfied;
- statistically and operationally meaningful objective improvement;
- bounded tail regression;
- bounded controller overhead;
- success across each declared validity segment.

Do not average away a severe regression in one operator family or hardware class.

## 10. Composing multiple controllers

Independent agents can create positive feedback: a partition controller increases task size, a concurrency controller reduces admission, a router sees low utilization and adds work, and all three invalidate each other’s estimates.

Use a shared coordinator with explicit ownership:

1. correctness and resource invariants;
2. executor safety/admission;
3. legal plan/stage choices;
4. performance optimization among safe candidates;
5. fleet/cost routing.

Each proposal should include affected resources, expected benefit, confidence, action lifetime, and invalidated models. Resolve coupled knobs jointly or serialize changes so attribution remains possible. A controller must observe the versions/actions of other controllers that change its plant.

Useful composition patterns include:

- safety controller exposes a feasible region; performance controller optimizes inside it;
- slow loop supplies priors; fast loop adapts locally but cannot overwrite history until validated;
- one coordinator selects from complete candidate configurations;
- controllers operate on disjoint resources with monitored coupling residuals.

Start with deterministic coordination. Multi-agent reinforcement learning is not a prerequisite and is hard to validate safely in a delayed, non-stationary distributed system.

## 11. Cross-run learning

Cross-run history is valuable for quantities that cannot be calculated at the current decision boundary. It may initialize an application-wide setting, but it can also describe data and component rates used by a later per-scan or per-stage runtime decision. SQL Server’s official [Memory Grant Feedback documentation](https://learn.microsoft.com/en-us/sql/relational-databases/performance/intelligent-query-processing-memory-grant-feedback) demonstrates the value of multiple-execution percentile history and persistence; Spark cloud services also use historical workloads for autotuning. These are precedents for the pattern, not parameter transfer proofs.

Persist two different structures:

- exact evidence provenance: observed query/plan, data/schema/snapshot, literal predicates, action, effective configuration, resource state, outcomes, and all versions;
- composable prediction observations: component feature schema and units, learned target/rate, uncertainty, sample weight/age, and compatible fallback levels.

Exact provenance is for audit and experimental replay, not a mandatory prediction match. At prediction time, each component uses the smallest causal feature subset available at the legal runtime boundary. Query literals, snapshots, schema additions, and executor-count changes normally change features and uncertainty; they do not invalidate unrelated components. Hard-invalidate only an affected component when metric meaning or mechanism is incompatible. Missing optional features widen uncertainty, choose a transferable prior, or disable that component while other components continue.

Maintain an exploration budget; otherwise a policy can become stuck on yesterday’s best choice.

Bayesian optimization and learned policies are appropriate for expensive cross-run search after the safe candidate space is defined. The [OtterTune paper](https://www.cs.cmu.edu/~dvanaken/papers/ottertune-sigmod17.pdf) shows transfer and sample-efficient database configuration tuning, but its results do not imply that the same feature set or gains transfer to GPU Spark jobs.

## 12. Workflow for engineers and agents

### Phase A: diagnose

- Freeze the optimization contract.
- Capture the effective plan, configuration, topology, and raw metrics.
- Build the dataflow and critical-path model.
- Classify the binding resource and quantify unexplained time.
- Produce at most three falsifiable hypotheses.

Use the [`gpu-tuning-diagnose`](../../skills/gpu-tuning-diagnose/SKILL.md) skill for this phase.

### Phase B: design and test

- Trace the candidate control surface and decision boundary.
- Write the model card and dimensional analysis.
- Define baseline, treatment, hold-out, safety cases, and success criteria.
- Run controlled experiments and retain raw artifacts.
- Reject or narrow the hypothesis when results disagree.

Use the [`gpu-tuning-model-design`](../../skills/gpu-tuning-model-design/SKILL.md) and [`gpu-tuning-experiment`](../../skills/gpu-tuning-experiment/SKILL.md) skills.

### Phase C: implement

- Add the smallest in-scope actuator/model change.
- Add observability before enabling control.
- Preserve resource hygiene and retry semantics.
- Update all applicable Spark shims.
- Add unit, integration, failure, and compatibility tests.
- Start disabled or shadowed when behavior is not already proven.
- Keep exact evidence provenance separate from component feature matching.
- Define per-component fallbacks so missing query, schema, plan, hardware, or live-state
  features degrade only the dependent prediction.

Use the [`gpu-tuning-model-lifecycle`](../../skills/gpu-tuning-model-lifecycle/SKILL.md)
and [`gpu-tuning-implement`](../../skills/gpu-tuning-implement/SKILL.md) skills.

### Phase D: review and promote

- Independently trace actual behavior from code.
- Audit units, timing, safety, controller interactions, and claims.
- Reproduce representative results.
- Approve only within the measured validity region.

Use the [`gpu-tuning-controller-review`](../../skills/gpu-tuning-controller-review/SKILL.md) skill.

## 13. Agent rules

An implementation agent must:

- inspect code and authoritative sources before asserting capability;
- distinguish observation, inference, and proposal;
- cite the symbol and lifecycle for every actuator;
- preserve raw evidence and report failed experiments;
- check units algebraically;
- state confidence and validity boundaries;
- avoid inventing configurations or contracts;
- avoid changing multiple coupled knobs in an attribution experiment;
- treat GPU resource ownership, OOM retry, and shims as correctness concerns;
- request missing evidence rather than fabricate it.

An agent must stop and ask for information when workload identity, correctness contract, authority to change external systems, or an essential experiment input cannot be discovered. It should not stop merely because a task is difficult; code tracing, source validation, and safe read-only inspection should be exhausted first.

## 14. Research backlog ordered by leverage

1. **Validate the current dynamic admission controller.** It exists, is broadly applicable, and has clear measurable hypotheses.
2. **Build a unified tuning evidence schema.** Align event-log, RAPIDS task, GPU/device, network, storage, plan, and configuration timelines.
3. **Quantify partition-to-batch-to-memory distributions.** This unlocks safer scan and AQE target models.
4. **Prototype GPU-aware AQE in shadow mode.** Recommend targets and compare them with realized downstream memory/throughput before changing plans.
5. **Create a cross-run prior store with strict identity/invalidation.** Use it first for startup-only and planning-time decisions.
6. **Evaluate compression and spill paths from complete byte accounting.** Avoid codec decisions based on ratio alone.
7. **Explore plan changes only after semantic and lifecycle integration is explicit.** Join/Bloom and operator-order changes have a larger correctness surface.
8. **Treat pipelined exchange/router/fleet work as separate architecture programs.** They require different ownership and failure models.

## 15. Source and validation notes

The synthesis was checked against:

- current repository code and generated configuration documentation, especially `GpuSemaphore`, `PrioritySemaphore`, `RapidsConf`, task metrics, spill/retry paths, chunked readers, and AQE-related tests;
- Apache Spark’s official AQE and configuration documentation;
- NVIDIA RAPIDS tuning, FAQ, and Auto-Tuner documentation;
- the claims, equations, diagrams, slide text, and presenter notes in `REPORT_1.md`, `REPORT_2.md`, `ProductArchitecture.pptx`, and `MoreProductArchitecture.pptx`;
- the local [dynamic-admission experiment](../experiments/dynamic-gpu-admission/README.md), whose manifest separates instrumentation evidence from performance claims.

The external links below were fetched over HTTPS with the web retrieval tool on 2026-07-03. The short excerpts are provenance anchors, not substitutes for reading each source.

| Source | Provenance excerpt (verbatim) | Claim supported / limit |
|---|---|---|
| [Roofline (OSTI record)](https://www.osti.gov/biblio/1407078) | “visual performance model” | Kernel performance ceiling/diagnostic; not a Spark controller. |
| [Hierarchical Roofline](https://arxiv.org/abs/2009.05257) | “Nsight Compute based method” | Empirical multi-level kernel analysis; preprint, not stage scheduling. |
| [Ridgeline](https://arxiv.org/abs/2209.01368) | “compute, memory, and network limits” | Distributed bottleneck classification; preprint, no Spark lifecycle. |
| [Theseus](https://arxiv.org/abs/2508.05029) | “Specialized asynchronous control mechanisms” | Accelerator-native asynchronous execution precedent; preprint, not a Spark implementation. |
| [Microsoft Memory Grant Feedback](https://learn.microsoft.com/en-us/sql/relational-databases/performance/intelligent-query-processing-memory-grant-feedback?view=sql-server-ver17) | “high percentile of past memory grant sizing requirements” | Precedent for persisted percentile feedback; does not justify RAPIDS p80. |
| [OtterTune primary paper](https://www.cs.cmu.edu/~dvanaken/papers/ottertune-sigmod17.pdf) | “reuse training data gathered from previous sessions” | Cross-session prior reuse; not direct evidence for Spark/GPU policies. |

No local performance benefit is claimed. The equations are model forms to validate, and the smoke test establishes adaptation only. Source-deck prototype and customer numbers were intentionally not repeated because the provided artifacts did not include sufficient immutable configuration, revision, raw-run, and repetition evidence to make them reproducible here.
