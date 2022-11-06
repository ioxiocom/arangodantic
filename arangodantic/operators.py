from enum import Enum


class Operators(str, Enum):
    """
    Enum class for different operators.

    See:
    https://www.arangodb.com/docs/stable/aql/operators.html#comparison-operators
    https://www.arangodb.com/docs/stable/aql/operators.html#array-comparison-operators
    for more details.
    """

    EQ = "=="
    NE = "!="
    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="
    IN = "IN"
    NOT_IN = "NOT IN"
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"
    REG_MATCH = "=~"
    NOT_REG_MATCH = "!~"
    NOT = "NOT"
    ALL_IN = "ALL IN"
    NONE_IN = "NONE IN"
    ANY_IN = "ANY IN"
    # These array comparison operators requires an extra operator
    # which will compare each individual array element.
    ANY_EQ = "ANY =="
    ANY_NE = "ANY !="
    ANY_LT = "ANY <"
    ANY_LTE = "ANY <="
    ANY_GT = "ANY >"
    ANY_GTE = "ANY >="
    ALL_EQ = "=="
    ALL_NE = "!="
    ALL_LT = "ALL <"
    ALL_LTE = "ALL <="
    ALL_GT = "ALL >"
    ALL_GTE = "ALL >="
    NONE_EQ = "NONE =="
    NONE_NE = "NONE !="
    NONE_LT = "NONE <"
    NONE_LTE = "NONE <="
    NONE_GT = "NONE >"
    NONE_GTE = "NONE >="


# List of supported operators mapped to a-z string representations that can be
# used safely in the names of bind_vars in AQL
comparison_operators = {
    Operators.EQ: "eq",
    Operators.NE: "ne",
    Operators.LT: "lt",
    Operators.LTE: "lte",
    Operators.GT: "gt",
    Operators.GTE: "gte",
    Operators.IN: "in",
    Operators.NOT_IN: "not_in",
    Operators.LIKE: "like",
    Operators.NOT_LIKE: "not_like",
    Operators.REG_MATCH: "reg_match",
    Operators.NOT_REG_MATCH: "not_reg_match",
    Operators.NOT: "not",
    Operators.ALL_IN: "all_in",
    Operators.NONE_IN: "none_in",
    Operators.ANY_IN: "any_in",
    Operators.ANY_EQ: "any_eq",
    Operators.ANY_NE: "any_ne",
    Operators.ANY_LT: "any_lt",
    Operators.ANY_LTE: "any_lte",
    Operators.ANY_GT: "any_gt",
    Operators.ANY_GTE: "any_gte",
    Operators.ALL_EQ: "all_eq",
    Operators.ALL_NE: "all_ne",
    Operators.ALL_LT: "all_lt",
    Operators.ALL_LTE: "all_lte",
    Operators.ALL_GT: "all_gt",
    Operators.ALL_GTE: "all_gte",
    Operators.NONE_EQ: "none_eq",
    Operators.NONE_NE: "none_ne",
    Operators.NONE_LT: "none_lt",
    Operators.NONE_LTE: "none_lte",
    Operators.NONE_GT: "none_gt",
    Operators.NONE_GTE: "none_gte",
}
