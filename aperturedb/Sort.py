from enum import Enum


class Order(Enum):
    ASCENDING = "ascending"
    DESCENDING = "descending"


class Sort():
    """
    **Specification of the sort order**
    """

    def __init__(self, key: str, order: Order) -> None:
        self.keys = [key]
        self.orders = [order]

    @property
    def _sort(self):
        if len(self.keys) == 1:
            return {
                "key": self.keys[0],
                "order": self.orders[0].value
            }
        return [{"key": k, "order": o.value} for k, o in zip(self.keys, self.orders)]

    def append(self, key: str, order: Order):
        self.keys.append(key)
        self.orders.append(order)
        return self
