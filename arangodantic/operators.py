from enum import Enum


class Operators(str, Enum):
    """
    Enum class for different operators.

    See:
    https://www.arangodb.com/docs/stable/aql/operators.html#comparison-operators
    https://www.arangodb.com/docs/stable/aql/operators.html#array-comparison-operators
    for more details.
    """

    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="
    NE = "!="
    EQ = "=="
    IN = "IN"
    NOT_IN = "NOT IN"
    ALL_IN = "ALL IN"
    NONE_IN = "NONE IN"
    ANY_IN = "ANY IN"
    ANY = "ANY"
    ALL = "ALL"
    NONE = "NONE"


# List of supported operators mapped to a-z string representations that can be
# used safely in the names of bind_vars in AQL
comparison_operators = {
    Operators.LT: "lt",
    Operators.LTE: "lte",
    Operators.GT: "gt",
    Operators.GTE: "gte",
    Operators.NE: "ne",
    Operators.EQ: "eq",
    Operators.IN: "in",
    Operators.NOT_IN: "not_in",
    Operators.ALL_IN: "all_in",
    Operators.NONE_IN: "none_in",
    Operators.ANY_IN: "any_in",
    Operators.ANY: "any",
    Operators.ALL: "all",
    Operators.NONE: "none",
}

# List of array comparison operators.
array_comparison_operators = {
    Operators.ALL_IN,
    Operators.NONE_IN,
    Operators.ANY_IN,
    Operators.ANY,
    Operators.ALL,
    Operators.NONE,
}
