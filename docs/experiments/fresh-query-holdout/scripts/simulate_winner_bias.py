#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Quantify apparent point-median regret when all candidates are truly equal."""

import argparse
import json
import math
import os
import random
import statistics


def percentile(values, fraction):
    values.sort()
    return values[round(fraction * (len(values) - 1))]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cv", type=float, default=0.06)
    parser.add_argument("--candidates", type=int, default=5)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--trials", type=int, default=100000)
    parser.add_argument("--seed", type=int, default=20260704)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)

    rng = random.Random(args.seed)
    sigma = math.sqrt(math.log1p(args.cv * args.cv))
    regrets = []
    for _ in range(args.trials):
        medians = [
            statistics.median(
                math.exp(rng.gauss(-0.5 * sigma * sigma, sigma))
                for _ in range(args.repetitions))
            for _ in range(args.candidates)
        ]
        regrets.append(medians[0] / min(medians) - 1.0)
    result = {
        "assumption": "independent equal lognormal candidates; candidate zero is fixed",
        "cv": args.cv,
        "candidates": args.candidates,
        "repetitions_per_candidate": args.repetitions,
        "trials": args.trials,
        "seed": args.seed,
        "mean_apparent_regret": statistics.mean(regrets),
        "median_apparent_regret": statistics.median(regrets),
        "p90_apparent_regret": percentile(regrets, 0.90),
        "p95_apparent_regret": percentile(regrets, 0.95),
        "probability_above_10_percent": sum(value > 0.10 for value in regrets) / len(regrets),
    }
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
