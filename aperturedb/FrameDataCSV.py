from aperturedb.ImageDataCSV import ImageDataCSV


class FrameDataCSV(ImageDataCSV):
    def __init__(self, *args, **kwargs):
        self.command = "AddFrame"
        super().__init__(*args, **kwargs)

    def get_indices(self):
        return {
            "entity": {
                "_Frame": self.get_indexed_properties()
            }
        }
