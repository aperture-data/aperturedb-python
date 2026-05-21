import pytest
from aperturedb.Operations import Operations


class TestOperations:
    def test_resize_width_height(self):
        op = Operations().resize(width=100, height=200)
        assert op.get_operations_arr() == [
            {"type": "resize", "width": 100, "height": 200}]

    def test_resize_scale(self):
        op = Operations().resize(scale=0.5)
        assert op.get_operations_arr() == [{"type": "resize", "scale": 0.5}]

    def test_rotate(self):
        op = Operations().rotate(angle=90)
        assert op.get_operations_arr() == [
            {"type": "rotate", "angle": 90, "resize": False}]

    def test_rotate_resize(self):
        op = Operations().rotate(angle=90, resize=True)
        assert op.get_operations_arr() == [
            {"type": "rotate", "angle": 90, "resize": True}]

    def test_flip(self):
        op = Operations().flip(code="horizontal")
        assert op.get_operations_arr() == [
            {"type": "flip", "code": "horizontal"}]

    def test_crop(self):
        op = Operations().crop(x=10, y=20, width=100, height=200)
        assert op.get_operations_arr() == [
            {"type": "crop", "x": 10, "y": 20, "width": 100, "height": 200}]

    def test_interval(self):
        op = Operations().interval(start=0, stop=100, step=2)
        assert op.get_operations_arr() == [
            {"type": "interval", "start": 0, "stop": 100, "step": 2}]

    def test_threshold(self):
        op = Operations().threshold(value=128)
        assert op.get_operations_arr() == [{"type": "threshold", "value": 128}]

    def test_preview(self):
        op = Operations().preview(max_frame_count=10, max_time_fraction=0.5)
        assert op.get_operations_arr() == [
            {"type": "preview", "max_frame_count": 10, "max_time_fraction": 0.5}]

    def test_preview_all_args(self):
        op = Operations().preview(max_frame_count=10, max_time_fraction=0.5, max_time_offset="00:00:10", max_size_mb=10.5)
        assert op.get_operations_arr() == [
            {"type": "preview", "max_frame_count": 10, "max_time_fraction": 0.5, "max_time_offset": "00:00:10", "max_size_mb": 10.5}]

    def test_chained_operations(self):
        op = Operations().resize(width=100, height=100).rotate(
            angle=90).crop(x=0, y=0, width=50, height=50)
        assert op.get_operations_arr() == [
            {"type": "resize", "width": 100, "height": 100},
            {"type": "rotate", "angle": 90, "resize": False},
            {"type": "crop", "x": 0, "y": 0, "width": 50, "height": 50}
        ]

    def test_resize_invalid_args(self):
        with pytest.raises(ValueError, match="Provide either 'scale' or both 'width' and 'height'"):
            Operations().resize()
        with pytest.raises(ValueError, match="Provide either 'scale' or both 'width' and 'height'"):
            Operations().resize(width=100)
        with pytest.raises(ValueError, match="Provide either 'scale' or both 'width' and 'height', but not a mix"):
            Operations().resize(width=100, height=100, scale=0.5)
