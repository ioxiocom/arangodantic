import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from arangodantic.directions import DIRECTIONS

FilterTypes = Optional[Dict[str, Any]]
SortTypes = Optional[Iterable[Tuple[str, str]]]

ONE_OR_MORE_DOTS_PATTERN = re.compile(r"\.+")


def build_filters(
    filters: FilterTypes, instance_name: str
) -> Tuple[List[str], Dict[str, str]]:
    """
    Turn filters into a list of AQL FILTER statements (using bind_vars) and
    corresponding bind_vars.

    Example:
    >>> build_filters({"owner.name": "John", "founded": {">=": 2000}}, "i")
    (
        [
            'i.@field_0_0.@field_0_1 == @field_0_eq',
            'i.@field_1_0 >= @field_1_gte'
        ],
        {
            'field_0_0': 'owner',
            'field_0_1': 'name',
            'field_0_eq': 'John',
            'field_1_0': 'founded',
            'field_1_gte': 2000
        }
    )
    """
    from arangodantic.models import Model

    filter_list = []
    bind_vars = {}

    # List of supported operators mapped to a-z string representations that can be
    # used safely in the names of bind_vars in AQL
    comparison_operators = {
        "<": "lt",
        "<=": "lte",
        ">": "gt",
        ">=": "gte",
        "!=": "ne",
        "==": "eq",
    }

    if filters:
        for i, (field, expr) in enumerate(filters.items()):
            if not isinstance(expr, Dict):
                # Convert literal value to an explicit {"==": value} expression to
                # simplify next steps
                expr = {"==": expr}

            for operator, value in expr.items():
                if operator not in comparison_operators:
                    raise NotImplementedError(
                        f"Support for '{operator}' not implemented"
                    )

                bind_var_prefix = f"field_{i}"

                # For left side of comparison
                field_str, field_bind_vars = split_field(field, prefix=bind_var_prefix)
                bind_vars.update(field_bind_vars)

                # For right side of comparison
                value_bind_var = f"{bind_var_prefix}_{comparison_operators[operator]}"
                if isinstance(value, Model):
                    # Make it possible to compare a field to a model; handy for
                    # example to match the "_from" or "_to" of an edge to a model.
                    value = value.id_
                bind_vars[value_bind_var] = value

                # The actual comparison
                filter_list.append(
                    f"{instance_name}.{field_str} {operator} @{value_bind_var}"
                )

    return filter_list, bind_vars


def split_field(name: str, prefix: str) -> Tuple[str, Dict[str, str]]:
    """
    Split the name of a field at each dot into a new bind_var.
    Returns a new string that is using bind vars to refer to the original field and
    corresponding bind_vars.

    Example:
        >>> split_field("owner.name", prefix="field_0")
        (
            '@field_0_0.@field_0_1',
            {
                'field_0_0': 'owner',
                'field_0_1': 'name'
            }
        )

    :param name: The name of the field.
    :param prefix: Prefix to use for all the generated bind_vars.
    """

    # Treat consecutive dots as a single dot
    name = ONE_OR_MORE_DOTS_PATTERN.sub(".", name)
    # Skip any leading or trailing dots
    name = name.strip(".")

    new_parts = []
    bind_vars = {}
    parts = name.split(".")
    for i, value in enumerate(parts):
        bind_var = f"{prefix}_{i}"
        new_parts.append(f"@{bind_var}")
        bind_vars[bind_var] = value

    new_str = ".".join(new_parts)

    return new_str, bind_vars


def remove_whitespace_lines(text: str) -> str:
    """
    Remove lines that only contains whitespace from a string.
    """
    return "\n".join([line for line in text.splitlines() if line.strip()])


def build_sort(
    instance_name: str, sort: SortTypes = None
) -> Tuple[str, Dict[str, str]]:
    """
    Turn the sort specification into an AQL SORT clause and corresponding bind_vars.

    :param instance_name: The name we're using in the AQL query for the instances we're
    looping over.
    :param sort: An Iterable containing tuples of fields and directions.
    :return: A tuple of the "SORT ..." string for use in AQL and the related bind_vars
    as a dictionary.
    """
    sort_lst = []
    bind_vars = {}
    if sort:
        for i, (field, direction) in enumerate(sort):
            if direction not in DIRECTIONS:
                raise ValueError(
                    f"Invalid sort direction '{direction}' for field {field}"
                )
            field_str, field_bind_vars = split_field(field, f"sort_{i}")
            bind_vars.update(field_bind_vars)
            sort_lst.append(f"{instance_name}.{field_str} {direction}")

    sort_str = ""
    if sort_lst:
        sort_str += "SORT " + ", ".join(sort_lst)

    return sort_str, bind_vars
