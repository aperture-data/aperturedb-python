# queryMessage.py - wraps protobuf versions
import google.protobuf

if google.protobuf.__version__.split(".")[0] == "3":
    from . import queryMessage3_pb2

    def queryMessage():
        return queryMessage3_pb2.queryMessage()

    def ParseFromString(msg, data):
        return msg.ParseFromString(data)
elif google.protobuf.__version__.split(".")[0] == "4":
    from . import queryMessage4_pb2

    def queryMessage():
        return queryMessage4_pb2.queryMessage()

    def ParseFromString(msg, data):
        # because of https://github.com/protocolbuffers/protobuf/issues/10774
        return msg.ParseFromString(memoryview(data).tobytes())
elif google.protobuf.__version__.split(".")[0] == "5":
    from . import queryMessage5_pb2

    def queryMessage():
        return queryMessage5_pb2.queryMessage()

    def ParseFromString(msg, data):
        return msg.ParseFromString(data)
else:
    raise Exception(
        f"aperturedb not compatible with {google.protobuf.__version__}")
