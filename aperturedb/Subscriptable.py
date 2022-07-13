class Subscriptable():
    """
    The base class to use for Data/Generators and such collection types.
    """

    def __getitem__(self, subscript):
        batch_context = {}
        if isinstance(subscript, slice):
            start = subscript.start if subscript.start else 0
            stop = subscript.stop if subscript.stop else len(self)
            step = subscript.step if subscript.step else 1
            return [self.getitem(i, batch_context) for i in range(start, stop, step)]
        else:
            return self.getitem(subscript, batch_context)

    def getitem(self, subscript, batch_context):
        raise Exception("To be implemented in subclass")
