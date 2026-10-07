from __future__ import annotations
from enum import Enum
from typing import Any, List, Optional


class Conjunction(Enum):
    AND = "all"
    OR = "any"


def literal(value: Any) -> Any:
    """
    Convert a value into an expression-array literal.

    Strings starting with ``$`` are escaped so they are not read as property
    references, ``_date`` wrappers become ``to_date`` calls, and lists become
    ``array`` constructors.
    """
    if isinstance(value, str) and value.startswith("$"):
        return "$" + value
    if isinstance(value, dict) and len(value) == 1 and ("_date" in value or "date" in value):
        return {"to_date": literal(next(iter(value.values())))}
    if isinstance(value, (list, tuple)):
        return {"array": [literal(v) for v in value]}
    if isinstance(value, dict):
        return {"json": value}
    return value


def predicate(key: str, op: str, value: Any) -> list:
    """Build a single ``["$key", op, value]`` comparison."""
    return ["$" + key, op, literal(value)]


def to_expression(constraints) -> Optional[list]:
    """
    Convert property-keyed constraints into an expression array.

    ``{"age": [">=", 20, "<=", 90], "name": ["==", "x"]}`` becomes
    ``["all", ["$age", ">=", 20], ["$age", "<=", 90], ["$name", "==", "x"]]``.
    Nested ``all``/``any`` groups are converted recursively. Expression arrays
    are returned unchanged, and empty constraints return None.
    """
    if constraints is None or isinstance(constraints, list):
        return constraints
    return _to_expression_group(constraints, Conjunction.AND.value)


def _to_expression_group(constraints: dict, conjunction: str) -> Optional[list]:
    terms = []
    for key, ops in constraints.items():
        if key in (Conjunction.AND.value, Conjunction.OR.value) and isinstance(ops, dict):
            group = _to_expression_group(ops, key)
            if group is not None:
                terms.append(group)
        else:
            comparisons = [predicate(key, ops[i], ops[i + 1])
                           for i in range(0, len(ops), 2)]
            # Both bounds of a property range must hold, even within an OR.
            if conjunction == Conjunction.OR.value and len(comparisons) > 1:
                terms.append([Conjunction.AND.value] + comparisons)
            else:
                terms.extend(comparisons)
    if not terms:
        return None
    if len(terms) == 1:
        return terms[0]
    return [conjunction] + terms


class Constraints(object):
    """
    **Constraints object for the Object mapper API**
    """

    def __init__(self, conjunction: Conjunction = Conjunction.AND):
        self._conjunction = conjunction.value
        self._predicates: List[tuple] = []

    @property
    def constraints(self) -> Optional[list]:
        """The constraints as an expression array, or None if empty."""
        if not self._predicates:
            return None
        return [self._conjunction] + [predicate(*p) for p in self._predicates]

    def _add(self, key, op, value) -> Constraints:
        self._predicates.append((key, op, value))
        return self

    def equal(self, key, value) -> Constraints:
        return self._add(key, "==", value)

    def notequal(self, key, value) -> Constraints:
        return self._add(key, "!=", value)

    def greaterequal(self, key, value) -> Constraints:
        return self._add(key, ">=", value)

    def greater(self, key, value) -> Constraints:
        return self._add(key, ">", value)

    def lessequal(self, key, value) -> Constraints:
        return self._add(key, "<=", value)

    def less(self, key, value) -> Constraints:
        return self._add(key, "<", value)

    def is_in(self, key, val_array) -> Constraints:
        return self._add(key, "in", val_array)

    def check(self, entity):
        results = []
        for key, op, value in self._predicates:
            if key not in entity:
                results.append(False)
            elif op == "==":
                results.append(entity[key] == value)
            elif op == "!=":
                results.append(entity[key] != value)
            elif op == ">=":
                results.append(entity[key] >= value)
            elif op == ">":
                results.append(entity[key] > value)
            elif op == "<=":
                results.append(entity[key] <= value)
            elif op == "<":
                results.append(entity[key] < value)
            elif op == "in":
                results.append(entity[key] in value)
            else:
                raise Exception("invalid constraint operation: " + op)
        if self._conjunction == Conjunction.OR.value:
            return any(results)
        return all(results)
