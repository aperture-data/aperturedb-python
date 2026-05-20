from enum import Enum


class Order(Enum):
    ASCENDING = "ascending"
    DESCENDING = "descending"


class Sort():
    """
    **Specification of the sort order**

    This class encapsulates sorting instructions for query results.
    It supports sorting by a single key or multiple keys. Additional
    sort keys can be added using the `append` method.
    """

    def __init__(self, key: str, order: Order) -> None:
        self.keys = [key]
        self.orders = [order]

    @property
    def _sort(self):
        if len(self.keys) != len(self.orders):
            raise ValueError("Number of keys and orders must match.")
        if len(self.keys) == 1:
            return {
                "key": self.keys[0],
                "order": self.orders[0].value
            }
        return [{"key": k, "order": o.value} for k, o in zip(self.keys, self.orders)]

    def append(self, key: str, order: Order) -> "Sort":
        """
        Append an additional sorting key and order.

        Args:
            key: The property key to sort by.
            order: The sort order (Order.ASCENDING or Order.DESCENDING).

        Returns:
            The current Sort instance to allow method chaining.
        """
        self.keys.append(key)
        self.orders.append(order)
        return self
