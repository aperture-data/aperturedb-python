from aperturedb import Subscriptable


class QueryGenerator(Subscriptable.Subscriptable):
    """
    The base class to use for Query Generators.
    """

    def getitem(self, subscript):
        raise Exception("To be implemented in subclass")
