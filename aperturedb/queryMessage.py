# queryMessage.py - wraps protobuf versions
import google.protobuf

if google.protobuf.__version__.split(".")[0] == "3":
    from . import queryMessage3_pb2
    print("Selected v3")

    def queryMessage():
        return queryMessage3_pb2.queryMessage()
elif google.protobuf.__version__.split(".")[0] == "4":
    from . import queryMessage4_pb2
    print("Selected v4")

    def queryMessage():
        return queryMessage4_pb2.queryMessage()
else:
    raise Exception(
        f"aperturedb not compatible with {google.protobuf.__version__}")
