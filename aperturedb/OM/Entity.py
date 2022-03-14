class Entity:
    """**This is the base class for the various ApertureDB Objects.**

    ApertureDB objects:
        * Entity
        * Image
        * Video
        * Blob
        * DescriptorSet
        * Descriptor
        * BoundingBox
    """
    def __init__(self) -> None:
        pass

    def save(self):
        """**Persists the current python representation of of the DB object**

        This might result in an Update/Add command, depending on wether the object is being
        newly created or modified.
        """
        pass