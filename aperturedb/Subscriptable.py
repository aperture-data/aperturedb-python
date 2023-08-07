class Wrapper():
    """
    This is needed because slicing in Subscriptable returns a list.
    The response handler also needs to be accounted for as
    that will be a part of generator.
    """

    def __init__(self, list, response_handler, strict_response_validation, blobs_relative_to_csv):
        self.list = list
        self.response_handler = response_handler
        self.strict_response_validation = strict_response_validation
        self.blobs_relative_to_csv = blobs_relative_to_csv

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
            start = len(self) + start if start < 0 else start
            stop = subscript.stop if subscript.stop else len(self)
            step = subscript.step if subscript.step else 1
            wrapper = Wrapper(
                [self.getitem(i) for i in range(start, stop, step)],
                self.response_handler if hasattr(
                    self, "response_handler") else None,
                self.strict_response_validation if hasattr(
                    self, "strict_response_validation") else None,
                self.blobs_relative_to_csv if hasattr(
                    self, "blobs_relative_to_csv") else False
            )
            return wrapper

        else:
            if subscript < len(self):
                return self.getitem(subscript)
            else:
                raise StopIteration()

    def getitem(self, subscript):
        raise Exception("To be implemented in subclass")

    def __iter__(self):
        self.ind = 0
        return self

    def __next__(self):
        if self.ind >= len(self):
            raise StopIteration
        else:
            r = self.getitem(self.ind)
            self.ind += 1
            return r
