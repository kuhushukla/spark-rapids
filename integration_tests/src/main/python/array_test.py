# Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql
from data_gen import *
from pyspark.sql.types import *

# Once we support arrays as literals then we can support a[null] and
# negative indexes for all array gens. When that happens
# test_nested_array_index should go away and this should test with
# array_gens_sample instead
@pytest.mark.parametrize('data_gen', single_level_array_gens, ids=idfn)
def test_array_index(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'a[0]',
                'a[1]',
                'a[null]',
                'a[3]',
                'a[50]',
                'a[-1]'),
            conf=allow_negative_scale_of_decimal_conf)

# Once we support arrays as literals then we can support a[null] for
# all array gens. See test_array_index for more info
@pytest.mark.parametrize('data_gen', nested_array_gens_sample, ids=idfn)
def test_nested_array_index(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'a[0]',
                'a[1]',
                'a[3]',
                'a[50]'))


@pytest.mark.parametrize('data_gen', all_basic_gens + [decimal_gen_default, decimal_gen_scale_precision], ids=idfn)
def test_make_array(data_gen):
    (s1, s2) = gen_scalars_for_sql(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'array(a, b)',
                'array(b, a, null, {}, {})'.format(s1, s2)))

# Decimals with negative scale
@pytest.mark.parametrize('data_gen', single_level_array_gens, ids=idfn)
def test_orderby_array(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : unary_op_df(spark, data_gen),
        'array_table',
        'select array_table.a, array_table.a[0] as first_val from array_table order by first_val',
        conf=allow_negative_scale_of_decimal_conf)


@pytest.mark.parametrize('data_gen', [ArrayGen(ArrayGen(short_gen, max_length=10), max_length=10),
                                      ArrayGen(ArrayGen(string_gen, max_length=10), max_length=10)], ids=idfn)
def test_orderby_array_of_arrays(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
    lambda spark : unary_op_df(spark, data_gen),
        'array_table',
        'select array_table.a, array_table.a[0][0] as first_val from array_table order by first_val')


@pytest.mark.parametrize('data_gen', [ArrayGen(StructGen([['child0', byte_gen],
                                                          ['child1', string_gen],
                                                          ['child2', float_gen]]))], ids=idfn)
def test_orderby_array_of_structs(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : unary_op_df(spark, data_gen),
        'array_table',
        'select array_table.a, array_table.a[0].child0 as first_val from array_table order by first_val')