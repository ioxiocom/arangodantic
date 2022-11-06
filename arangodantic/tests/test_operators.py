from typing import Any, List, Union

import pytest

from arangodantic.operators import Operators
from arangodantic.tests.conftest import Identity


@pytest.mark.parametrize(
    "data, operator, query, expected_length",
    [
        (0, Operators.EQ, None, 0),
        (1, Operators.GT, 0, 1),
        (True, Operators.NE, None, 1),
        (45, Operators.LTE, "yikes!", 1),
        (65, Operators.NE, "65", 1),
        (65, Operators.EQ, 65, 1),
        (1.23, Operators.GT, 1.32, 0),
        (1.5, Operators.IN, [2, 3, 1.5], 1),
        ("foo", Operators.IN, None, 0),
        (42, Operators.NOT_IN, [17, 40, 50], 1),
        ("abc", Operators.EQ, "abc", 1),
        ("abc", Operators.EQ, "ABC", 0),
        ("foo", Operators.LIKE, "f%", 1),
        ("foo", Operators.NOT_LIKE, "f%", 0),
        ("foo", Operators.REG_MATCH, "^f[o].$", 1),
        ("foo", Operators.NOT_REG_MATCH, "[a-z]+bar$", 1),
    ],
)
@pytest.mark.asyncio
async def test_comparison_operators(
    identity_collection,
    data: Any,
    operator: Operators,
    query: Any,
    expected_length: int,
):
    # See https://www.arangodb.com/docs/stable/aql/operators.html#comparison-operators
    # for the test cases.
    i_a = Identity(name="a", data={"value": data})
    await i_a.save()

    cursor = await Identity.find({"data.value": {operator: query}}, count=True)
    async with cursor:
        assert len(cursor) == expected_length


@pytest.mark.parametrize(
    "data, operator, query, expected_length",
    [
        ([1, 2, 3], Operators.ALL_IN, [2, 3, 4], 0),
        ([1, 2, 3], Operators.ALL_IN, [1, 2, 3], 1),
        ([1, 2, 3], Operators.NONE_IN, [3], 0),
        ([1, 2, 3], Operators.NONE_IN, [23, 42], 1),
        ([1, 2, 3], Operators.ANY_IN, [4, 5, 6], 0),
        ([1, 2, 3], Operators.ANY_IN, [1, 42], 1),
        ([1, 2, 3], Operators.ANY_EQ, 2, 1),
        ([1, 2, 3], Operators.ANY_EQ, 4, 0),
        ([1, 2, 3], Operators.ANY_GT, 0, 1),
        ([1, 2, 3], Operators.ANY_LTE, 1, 1),
        ([1, 2, 3], Operators.ANY_LTE, 1, 1),
        ([1, 2, 3], Operators.NONE_LT, 99, 0),
        ([1, 2, 3], Operators.NONE_GT, 10, 1),
        ([1, 2, 3], Operators.ALL_GT, 2, 0),
        ([1, 2, 3], Operators.ALL_GT, 0, 1),
        ([1, 2, 3], Operators.ALL_GTE, 3, 0),
        (["foo", "bar"], Operators.ALL_NE, "moo", 1),
        (["foo", "bar"], Operators.NONE_EQ, "bar", 0),
        (["foo", "bar"], Operators.ANY_EQ, "foo", 1),
    ],
)
@pytest.mark.asyncio
async def test_array_comparison_operators(
    identity_collection,
    data: List[Union[int, str]],
    operator: Operators,
    query: Union[List[int], int],
    expected_length: int,
):
    # See
    # https://www.arangodb.com/docs/stable/aql/operators.html#array-comparison-operators
    # for the test cases.
    i_a = Identity(name="a", data={"list": data})

    await i_a.save()

    cursor = await Identity.find({"data.list": {operator: query}}, count=True)
    async with cursor:
        assert len(cursor) == expected_length
