import tempfile
import os
from aperturedb.FrameDataCSV import FrameDataCSV
from aperturedb.Query import ObjectType


def test_FrameDataCSV_command():
    with tempfile.NamedTemporaryFile(
        suffix=".csv", mode="w", delete=False
    ) as f:
        f.write("url,id\nhttp://example.com/frame.jpg,1\n")

    try:
        # We don't actually need the image since check_image=False
        frame_data = FrameDataCSV(f.name, check_image=False)

        cmd = frame_data.command
        assert cmd == "AddFrame", f"Expected AddFrame, got {cmd}"

        indices = frame_data.get_indices()
        assert "entity" in indices
        frame_type = ObjectType.FRAME.value
        assert frame_type in indices["entity"]
        assert indices["entity"][frame_type] == frame_data.get_indexed_properties()
    finally:
        os.remove(f.name)
