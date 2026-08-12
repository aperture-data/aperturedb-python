from aperturedb.ImageDataCSV import ImageDataCSV


class FrameDataCSV(ImageDataCSV):
    """
    **Helper class to ingest Frame data from a CSV file.**

    This class extends ImageDataCSV and sets the insertion command to "AddFrame",
    allowing frame files to be batch ingested from CSVs just like images.
    """
    command = "AddFrame"

    def get_indices(self):
        return {
            "entity": {
                "_Frame": self.get_indexed_properties()
            }
        }
