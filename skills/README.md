# Project Aether Agent Skills

This repository contains independent skill suites for the [RAPIDS Accelerator for Apache Spark](https://github.com/NVIDIA/spark-rapids). Aether Agent converts Apache Spark User-Defined Functions (UDFs) for GPU acceleration. It provides:

1. **Test generation** -- Create unit tests and test data for existing UDFs.
2. **Conversion** -- Convert a UDF to a GPU-compatible implementation (SQL, cuDF RapidsUDF, or native CUDA RapidsUDF).
3. **Benchmarking** -- Generate synthetic data and benchmark the original UDF against the GPU implementation.
4. **Optimization** -- Iteratively profile and optimize a cuDF RapidsUDF for GPU performance.

This repository also provides an evidence-driven workflow for dynamic GPU Spark tuning:

1. **[gpu-tuning-diagnose](gpu-tuning-diagnose/SKILL.md)** -- Reconstruct the critical path and produce falsifiable bottleneck hypotheses.
2. **[gpu-tuning-model-design](gpu-tuning-model-design/SKILL.md)** -- Compare baseline and proposed execution graphs for pipelining, MPP, compression, batching, and other new designs.
3. **[gpu-tuning-experiment](gpu-tuning-experiment/SKILL.md)** -- Design, version, run, and analyze controlled tuning experiments.
4. **[gpu-tuning-model-lifecycle](gpu-tuning-model-lifecycle/SKILL.md)** -- Turn historical evidence and live telemetry into versioned priors, challengers, drift decisions, and safe model updates.
5. **[gpu-tuning-implement](gpu-tuning-implement/SKILL.md)** -- Implement an authorized, validated controller change with observability, safety, shim coverage, and tests.
6. **[gpu-tuning-controller-review](gpu-tuning-controller-review/SKILL.md)** -- Independently audit actuator feasibility, model validity, controller safety, implementation, and evidence before promotion.

<details open>
<summary><strong>Table of Contents</strong></summary>

- [Installation](#installation)
- [Supported Formats](#supported-formats)
- [Prerequisites](#prerequisites)
- [Selecting an LLM](#selecting-an-llm)
- [Quick Start](#quick-start)
  - [Dynamic GPU Tuning](#dynamic-gpu-tuning)
  - [UDF Conversion](#udf-conversion)
  - [Try the UDF Workflow](#try-the-udf-workflow)

</details>

## Installation

Install via the [skills CLI](https://github.com/vercel-labs/skills). Install the UDF-conversion suite, the dynamic-tuning suite, or all skills according to the task; the two suites are independent.

```bash
npx skills add NVIDIA/spark-rapids --skill '*' [--agent <agent>]
```

## Supported Formats

| UDF Type  | cuDF RapidsUDF | CUDA RapidsUDF | Spark SQL |
|-----------|----------------|------------------------|-----------|
| Java UDF  | Yes | Yes | Yes |
| Hive UDF  | Yes | Yes | Yes |
| Scala UDF | Yes | Yes | Yes |
| Java UDAF | -- | -- | Yes |
| Hive UDAF | -- | -- | Yes |
| Scala UDAF | -- | -- | Yes |

## Prerequisites

For the UDF conversion workflow:

- **[Maven](https://maven.apache.org/install.html)** is required to build/compile UDFs.
- **[JDK](https://docs.oracle.com/en/java/javase/index.html)** must be installed on the system.
- **Local GPU** with [CUDA toolkit](https://developer.nvidia.com/cuda/toolkit) is required for GPU validation and benchmarking (see [Spark RAPIDS compatibility](https://nvidia.github.io/cudf-spark/docs/download.html) for version requirements). A compatible remote GPU environment can be used instead.

For dynamic tuning, source code and saved plans/logs are enough for diagnosis and review. GPU or cluster access is required only to execute experiments or GPU integration tests, and those actions also require environment/cost authority.

## Selecting an LLM

For best results, we recommend the latest reasoning models from OpenAI, Anthropic, or Google. As a good proxy, models near the top of the [Terminal-Bench 2.0 leaderboard](https://www.tbench.ai/leaderboard/terminal-bench/2.0) tend to perform well.

## Quick Start

Skills require any IDE or LLM that supports the [agent skills spec](https://agentskills.io) (e.g., Cursor, Codex, Claude Code).

### Dynamic GPU Tuning

Use the tuning skills as evidence gates:

1. **[gpu-tuning-diagnose](gpu-tuning-diagnose/SKILL.md)** -- define the objective, reconstruct the scheduled critical path, and rank falsifiable hypotheses.
2. **[gpu-tuning-model-design](gpu-tuning-model-design/SKILL.md)** -- model and prioritize new execution or optimization ideas; skip only when validating an existing controller without a design change.
3. **[gpu-tuning-experiment](gpu-tuning-experiment/SKILL.md)** -- pre-register and run an authorized, reproducible experiment.
4. **[gpu-tuning-model-lifecycle](gpu-tuning-model-lifecycle/SKILL.md)** -- define reusable history, live-state updates, drift, immutable challenger versions, and promotion gates.
5. **[gpu-tuning-implement](gpu-tuning-implement/SKILL.md)** -- implement the smallest justified controller change.
6. **[gpu-tuning-controller-review](gpu-tuning-controller-review/SKILL.md)** -- independently audit it before promotion.

Within this repository, see the [model-driven tuning guide](../docs/design/dynamic-gpu-job-tuning.md). Each skill remains self-contained when installed separately.

### UDF Conversion

The UDF skills follow a multi-step workflow:

1. **[udf-gen-test](udf-gen-test/SKILL.md)** -- Generate a unit test for the UDF
2. **[udf-convert-to-cudf](udf-convert-to-cudf/SKILL.md)**, **[udf-convert-to-cuda](udf-convert-to-cuda/SKILL.md)**, or **[udf-convert-to-sql](udf-convert-to-sql/SKILL.md)** -- Convert the UDF to a GPU-compatible implementation
3. **[udf-judge-conversion](udf-judge-conversion/SKILL.md)** -- Review generated tests and implementations for coverage gaps, bugs, and edge cases
4. **[udf-benchmark](udf-benchmark/SKILL.md)** -- Benchmark CPU vs GPU performance
5. **[udf-optimize-cudf](udf-optimize-cudf/SKILL.md)** -- Iteratively profile and optimize the cuDF RapidsUDF

To invoke a skill, use your IDE's skill command, or simply describe the task and let the agent load the skill automatically.

```bash
# Manual invocation
❯ Use the /udf-gen-test skill to generate a unit test for @FormatPhoneUDF.java

# Automatic invocation
❯ Generate a unit test for @FormatPhoneUDF.java
```

Each skill builds on the output of the previous one -- udf-gen-test produces a project with a passing unit test, which the conversion skills use as input, and the udf-benchmark skill uses the conversion output.

You can invoke multiple steps in a single prompt:

```bash
❯ Generate a unit test for @FormatPhoneUDF.java, then convert it to cuDF, native CUDA, or SQL and benchmark
```

### Try the UDF Workflow

Once you've installed the skills, try the workflow with one of the provided example UDFs:
- Java: [FormatPhoneUDF.java](examples/FormatPhoneUDF.java)
- Hive: [IntegerMultiplyBy2UDF.java](examples/IntegerMultiplyBy2UDF.java)
- Scala: [CalculateRiskUDF.scala](examples/CalculateRiskUDF.scala)

#### Workflow

(Example dialogue is from Claude Code).

1. Open your project in your IDE of choice with the skills installed.
2. Ask the agent to generate a unit test:

    ```bash
    ❯ Please generate a unit test for @FormatPhoneUDF.java
      ⎿  Read FormatPhoneUDF.java (23 lines)

    ● Skill(udf-gen-test)
      ⎿  Successfully loaded skill
    ● I'll generate a unit test for FormatPhoneUDF.java. Let me start by setting up the project.
    ● ...
    ● Unit test generation is complete. Here's a summary:

      Results

      - Project directory: FormatPhoneUDF/
      - Unit test: FormatPhoneUDF/src/test/scala/com/udf/UnitTest.scala
      - Status: All tests passing
      - Test coverage: 12 cases
   ```

3. Once the test passes, convert the UDF:

    ```bash
    ❯ Please convert the UDF to cuDF

    ● I'll use the udf-convert-to-cudf skill to convert this UDF to a GPU-accelerated RapidsUDF.
    ● Skill(udf-convert-to-cudf)
      ⎿  Successfully loaded skill
    ● ...
    ● Results

      The RapidsUDF conversion is complete.

      - RapidsUDF: FormatPhoneUDF/src/main/java/com/udf/FormatPhoneRapidsUDF.java
      - Comparison test: passes with no memory leaks
    ```

   Alternatively, ask for a judge pass as part of the conversion request:

    ```bash
    ❯ Please convert the UDF to cuDF and run the judge review agent on the result
    ```

   **Note:** The conversion agent will only invoke the judge when you explicitly ask for it. Otherwise, the agent performs a local self-review. You can also invoke the udf-judge-conversion skill on its own, at any time.

4. Finally, benchmark the result:

    ```bash
    ❯ Please benchmark the implementations on 100M rows.

    ● Skill(udf-benchmark)
      ⎿  Successfully loaded skill
    ● ...
    ● Benchmark Results — 100M rows

      ┌─────────┬─────────────┐
      │  Mode   │ Runtime (s) │
      ├─────────┼─────────────┤
      │ CPU     │ 16.27       │
      ├─────────┼─────────────┤
      │ GPU     │ 6.52        │
      ├─────────┼─────────────┤
      │ Speedup │ 2.50x       │
      └─────────┴─────────────┘

      The GPU RapidsUDF implementation is 2.5x faster than the CPU UDF on 100 million rows.
    ```

5. Optionally for cuDF RapidsUDF conversions, optimize the implementation:

    ```bash
    ❯ Please optimize the implementation

    ● Skill(udf-optimize-cudf)
      ⎿  Successfully loaded skill
    ● ...
    ```
