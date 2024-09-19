from typing import Annotated, Any
from aperturedb.Descriptors import Descriptors
from aperturedb.Images import Images
from aperturedb.Utils import create_connector
from aperturedb.Query import Query
from pydantic import BaseModel, Field


def x(*args, **kwargs):
    print(args)
    print(kwargs)
    pass


class B:
    def __call__(self, *args: Any, **kwds: Any) -> Any:
        print(args)


class X:
    y: int

    @classmethod
    def __gt__(cls, other):
        return cls.y > other


if __name__ == "__main__":
    images = Images.retrieve(create_connector(),
                             Query.spec(limit=10))
    for image in images:
        print(image)
        print("-----")

    descs = Descriptors.retrieve(create_connector(),
                                 Query.spec(limit=3))
    for desc in descs:
        print(desc)
        print("-----")
    # x(X.y > 45)
    # p = X(y=45)

    # c = B()(X.y>2)
