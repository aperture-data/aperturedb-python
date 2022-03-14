from typing import TypeVar, Generic
from logging import Logger

T = TypeVar('T')

class Dataset(Generic[T]):
    """**Collection of entities from apertureDB**
    
    It facilitates interaction with a collection of objects in the Database.

    Args:
        Generic (_type_): _description_
    """
    def __init__(self) -> None:
        self._object_type = "Entity"

    # The iterable interface for Dataset.
    def __iter__(self):
        pass

    def __next__(self) -> T:
        pass

    #The array interface
    def __len__(self) -> int:
        return 0
    
    def __getitem__(self) -> T:
        pass