from aperturedb.transformers.transformer import Transformer


class Foo(Transformer):
    """
    An example of a non packaged enrichment step.
    example usage in adb (The argument to cli is --user-enrich):
    adb ingest generate examples/CelebADataKaggle.py --sample-count 1 --user-enrich examples/Foo.py
    """

    def getitem(self, subscript):
        x = self.data[subscript]
        for ic in self._add_image_index:
            x[0][ic]["AddImage"]["properties"]["foo"] = "bar"

        return x
