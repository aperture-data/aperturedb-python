from aperturedb.ImageDataCSV import ImageDataCSV

class FrameDataCSV(ImageDataCSV):
    def __init__(self, *args, **kwargs):
        self.command = "AddFrame"
        super().__init__(*args, **kwargs)
