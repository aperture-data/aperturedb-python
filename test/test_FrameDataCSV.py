import pytest
import pandas as pd
import tempfile
import os
from aperturedb.FrameDataCSV import FrameDataCSV


def test_FrameDataCSV_command():
    with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as f:
        f.write("url,id\nhttp://example.com/frame.jpg,1\n")
        f.close()

        try:
            # We don't actually need the image since check_image=False
            frame_data = FrameDataCSV(f.name, check_image=False)

            assert frame_data.command == "AddFrame", f"Expected AddFrame, got {
                frame_data.command}"

            indices = frame_data.get_indices()
            assert "entity" in indices
            assert "_Frame" in indices["entity"]
            assert indices["entity"]["_Frame"] == frame_data.get_indexed_properties()
        finally:
            os.remove(f.name)
