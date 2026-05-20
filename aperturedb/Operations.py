from __future__ import annotations
from typing import Optional


class Operations(object):
    """
    **Operations that can be performed on the fly on any retrieved images**

    [Supported operations](/query_language/Reference/shared_command_parameters/operations)
    """

    def __init__(self):

        self.operations_arr = []

    def get_operations_arr(self):
        return self.operations_arr

    def resize(self, width: Optional[int] = None, height: Optional[int] = None, scale: Optional[float] = None) -> Operations:

        if scale is not None:
            if width is not None or height is not None:
                raise ValueError(
                    "Provide either 'scale' or both 'width' and 'height', but not a mix.")
        else:
            if width is None or height is None:
                raise ValueError(
                    "Provide either 'scale' or both 'width' and 'height'.")

        op = {
            "type": "resize"
        }
        if width is not None:
            op["width"] = width
            op["height"] = height
        if scale is not None:
            op["scale"] = scale

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

    def threshold(self, value: int) -> Operations:

        op = {
            "type": "threshold",
            "value": value,
        }

        self.operations_arr.append(op)
        return self

    def preview(self, max_frame_count: Optional[int] = None, max_time_fraction: Optional[float] = None, max_time_offset: Optional[str] = None, max_size_mb: Optional[float] = None) -> Operations:

        op = {
            "type": "preview"
        }
        if max_frame_count is not None:
            op["max_frame_count"] = max_frame_count
        if max_time_fraction is not None:
            op["max_time_fraction"] = max_time_fraction
        if max_time_offset is not None:
            op["max_time_offset"] = max_time_offset
        if max_size_mb is not None:
            op["max_size_mb"] = max_size_mb

        self.operations_arr.append(op)
        return self
