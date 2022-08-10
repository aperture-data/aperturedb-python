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

    def equal(self, key, value):

        self.constraints[self._conjunction][key] = ["==", value]

    def greaterequal(self, key, value):

        self.constraints[self._conjunction][key] = [">=", value]

    def greater(self, key, value):

        self.constraints[self._conjunction][key] = [">", value]

    def lessequal(self, key, value):

        self.constraints[self._conjunction][key] = ["<=", value]

    def less(self, key, value):

        self.constraints[self._conjunction][key] = ["<", value]

    def is_in(self, key, val_array):

        self.constraints[self._conjunction][key] = ["in", val_array]

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
