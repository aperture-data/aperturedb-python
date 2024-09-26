from aperturedb.Entities import Entities


class Clips(Entities):
    """
    **The object mapper representation of Video Clips in ApertureDB.**

    This class is a layer on top of the native query.
    It facilitates interactions with Video clips in the database in the pythonic way.
    """
    db_object = "_Clip"
