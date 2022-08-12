class Wrapper():
    """
    This is needed because slicing in Subscriptable returns a list.
    The response handler also needs to be accounted for as
    that will be a part of generator.
    """

    def __init__(self, list, response_handler):
        self.list = list
        self.response_handler = response_handler

    def __len__(self):
        return len(self.list)

    def __getitem__(self, i):
        return self.list[i]


class Subscriptable():
    """
    The base class to use for Data/Generators and such collection types.
    """

    def __getitem__(self, subscript):
        if isinstance(subscript, slice):
            start = subscript.start if subscript.start else 0
            stop = subscript.stop if subscript.stop else len(self)
            step = subscript.step if subscript.step else 1
            return Wrapper(
                [self.getitem(i) for i in range(start, stop, step)],
                self.response_handler if hasattr(self, "response_handler") else None)
        else:
            return self.getitem(subscript)

    def getitem(self, subscript):
        raise Exception("To be implemented in subclass")
