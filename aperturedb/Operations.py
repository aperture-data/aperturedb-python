from __future__ import annotations


class Operations(object):
    """
    **Operations that can be performed on the fly on any retrieved images**

    [Supported operations](/query_language/Reference/shared_command_parameters/operations)
    """

    def __init__(self):

        self.operations_arr = []

    def get_operations_arr(self):
        return self.operations_arr

    def resize(self, width: int, height: int) -> Operations:

        op = {
            "type": "resize",
            "width":  width,
            "height": height,
        }

        self.operations_arr.append(op)
        return self

    def rotate(self, angle: int, resize=False) -> Operations:

        op = {
            "type": "rotate",
            "angle": angle,
            "resize": resize,
        }

        self.operations_arr.append(op)
        return self

    def flip(self, code: str) -> Operations:

        op = {
            "type": "flip",
            "code": code,
        }

        self.operations_arr.append(op)
        return self

    def crop(self, x: int, y: int, width: int, height: int) -> Operations:

        op = {
            "type": "crop",
            "x": x,
            "y": y,
            "width": width,
            "height": height,
        }

        self.operations_arr.append(op)
        return self

    def interval(self, start: int, stop: int, step: int) -> Operations:

        op = {
            "type": "interval",
            "start": start,
            "stop": stop,
            "step": step
        }

        self.operations_arr.append(op)
        return self
