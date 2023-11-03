from aperturedb.transformers.transformer import Transformer


class Foo(Transformer):
    """
    An example of a non packaged transformer.
    example usage in adb (The argument to cli is --user-transformer)):
    adb from-generator examples/CelebADataKaggle.py --sample-count 1 --user-transformer examples/Foo.py
    """

    def getitem(self, subscript):
        x = self.data[subscript]
        for ic in self._add_image_index:
            x[0][ic]["AddImage"]["properties"]["foo"] = "bar"

        return x
