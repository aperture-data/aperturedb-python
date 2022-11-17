from __future__ import annotations
from enum import Enum


class Conjunction(Enum):
    AND = "all"
    OR = "any"


class Constraints(object):

    def __init__(self, conjunction: Conjunction = Conjunction.AND):
        self._conjunction = conjunction.value
        self.constraints = {
            conjunction.value: {
            }
        }

    def equal(self, key, value) -> Constraints:
        self.constraints[self._conjunction][key] = ["==", value]
        return self

    def notequal(self, key, value) -> Constraints:
        self.constraints[self._conjunction][key] = ["!=", value]
        return self

    def greaterequal(self, key, value) -> Constraints:
        self.constraints[self._conjunction][key] = [">=", value]
        return self

    def greater(self, key, value) -> Constraints:
        self.constraints[self._conjunction][key] = [">", value]
        return self

    def lessequal(self, key, value) -> Constraints:
        self.constraints[self._conjunction][key] = ["<=", value]
        return self

    def less(self, key, value) -> Constraints:
        self.constraints[self._conjunction][key] = ["<", value]
        return self

    def is_in(self, key, val_array) -> Constraints:
        self.constraints[self._conjunction][key] = ["in", val_array]
        return self

    def check(self, entity):
        for key, op in self.constraints.items():
            if key not in entity:
                return False
            if op[0] == "==":
                if not entity[key] == op[1]:
                    return False
            elif op[0] == ">=":
                if not entity[key] >= op[1]:
                    return False
            elif op[0] == ">":
                if not entity[key] > op[1]:
                    return False
            elif op[0] == "<=":
                if not entity[key] <= op[1]:
                    return False
            elif op[0] == "<":
                if not entity[key] < op[1]:
                    return False
            elif op[0] == "in":
                if not entity[key] in op[1]:
                    return False
            else:
                raise Exception("invalid constraint operation: " + op[0])
        return True
