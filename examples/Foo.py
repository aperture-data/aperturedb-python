from aperturedb.transformers.transformer import Transformer


class Foo(Transformer):
    """
    An example of a non packaged transformer.
    example usage in adb:
    adb from-generator examples/CelebADataKaggle.py --sample-count 1 --transformer examples/Foo.py
    """

    def getitem(self, subscript):
        x = self.data[subscript]
        for ic in self._add_image_index:
            x[0][ic]["AddImage"]["properties"]["foo"] = "bar"

        return x
