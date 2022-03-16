
from typing import List, TypeVar, Generic
from aperturedb.OM.Entity import Entity

T = TypeVar('T')

class Cursor(Generic[T]):
    """_summary_

    Notes:
        - This interface tries to match Dataset interface.

    Args:
        Generic (_type_): _description_
    """
    
    def __init__(self, items: List[T]) -> None:
        self.items = items

    def __repr__(self) -> None:
        return f"""
        Cursor with {len(self.items)} elements of type {T}
        """

    def __len__(self) -> int:
        return len(self.items)

    def __iter__(self):
        self._index = 0
        return self

    def __next__(self) -> T:
        if self._index < len(self.items) - 1:
            self._index += 1
            return self.items[self._index]
        else:
            raise StopIteration
    
    def associate_entities(self, entities: List[List[Entity]]):
        """**Helper method to easily add connections to graph**

        Pass a list of entities to be associated to the list in the cursor.

        Args:
            entities (Entity): The list of entities to be associated, where each item can be multiple entities.

        """
        assert(len(entities) == len(self.items))
