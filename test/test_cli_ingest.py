from unittest.mock import patch, MagicMock
from aperturedb.cli.ingest import from_csv, IngestType


def test_from_csv_frame_type():
    import aperturedb.FrameDataCSV

    with patch("aperturedb.cli.ingest._process_data") as mock_process_data:
        mock_process_data.return_value = None

        with patch.object(aperturedb.FrameDataCSV, "FrameDataCSV") as mock_csv_class:
            mock_data = MagicMock()
            mock_csv_class.return_value = mock_data

            from_csv(filepath="dummy.csv",
                     ingest_type=IngestType.FRAME, sample_count=5)

            mock_csv_class.assert_called_once_with(
                "dummy.csv", use_dask=False, blobs_relative_to_csv=True)
            mock_process_data.assert_called_once_with(
                mock_data,
                sample_count=5,
                module_name="dummy.csv",
                batchsize=1,
                num_workers=1,
                stats=True,
                debug=False
            )
