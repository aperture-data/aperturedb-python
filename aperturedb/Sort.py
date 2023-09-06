from enum import Enum


class Order(Enum):
    ASCENDING = "ascending"
    DESCENDING = "descending"


class Sort():
    """
    **Specification of the sort order**
    """

    def __init__(self, key: str, order: Order) -> None:
        self._sort = {
            "key": key,
            "order": order.value
        }
