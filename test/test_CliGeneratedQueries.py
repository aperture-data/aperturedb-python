"""Offline CLI query/CSV generation must use the selected server API."""
import copy
import csv
import os
from pathlib import Path
import struct
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from aperturedb.cli import ingest
from aperturedb.ConnectionDataCSV import ConnectionDataCSV


class CliGeneratedQueries(unittest.TestCase):
    def test_croissant_metadata_and_records_use_the_discovered_hello(self):
        record = SimpleNamespace(name="Records", uuid="records")
        dataset = SimpleNamespace(metadata=SimpleNamespace(record_sets=[record]),
                                  records=lambda **kw: iter([{"id": 1}]))
        for debug, version in [(False, 1), (False, 2), (True, 2)]:
            with self.subTest(debug=debug, version=version):
                persist = Mock(return_value=([], []))
                records = Mock(return_value=[([], [])])
                client = SimpleNamespace(hello_version=version, close=Mock())
                modules = {"mlcroissant": SimpleNamespace(Dataset=lambda _: dataset),
                           "aperturedb.MLCroissant": SimpleNamespace(
                               persist_metadata=persist, MLCroissantRecordSet=records)}
                with patch.dict(sys.modules, modules), patch.object(ingest, "_process_data"), \
                        patch("aperturedb.CommonLibrary.create_connector", return_value=client) as create:
                    ingest.from_croissant(
                        "test://dataset", debug=debug, hello_version=version)
                self.assertEqual(
                    persist.call_args.kwargs["hello_version"], version)
                self.assertEqual(
                    records.call_args.kwargs["hello_version"], version)
                self.assertEqual(create.call_count, 0 if debug else 1)
                self.assertEqual(client.close.call_count, 0 if debug else 1)

    def test_offline_embedding_csvs_emit_loadable_typed_endpoints_without_a_database(self):
        import numpy as np
        payload = struct.pack("2f", 1., 2.)

        class Properties:
            def __init__(self, data, **kwargs):
                self.data = data

            def __len__(self):
                return len(self.data)

            def __getitem__(self, index):
                commands, blobs = copy.deepcopy(self.data[index])
                commands[0]["AddImage"].setdefault("properties", {})[
                    "adb_image_sha256"] = "sha256"
                return commands, blobs

        class Embedding(Properties):
            def __init__(self, data, **kwargs):
                super().__init__(data)
                self.search_set_name = kwargs["search_set_name"]
                self._descriptorset_initialized = False

            def __getitem__(self, index):
                assert self._descriptorset_initialized, "Offline generation must not create an index"
                commands, blobs = super().__getitem__(index)
                commands.append({"AddDescriptor": {
                    "class" if self.hello_version == 2 else "set": self.search_set_name}})
                return commands, blobs + [payload]

        original = Path.cwd()
        try:
            for version in (1, 2):
                with self.subTest(version=version), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    os.chdir(root)
                    # The custom pipeline only needs raw bytes, never an ML model.
                    (root / "image.bin").write_bytes(b"image")
                    (root / "images.csv").write_text("filename\n" +
                                                     str(root / "image.bin") + "\n")
                    with patch.object(ingest, "_create_pipeline", return_value=[Properties, Embedding]), \
                            patch("aperturedb.CommonLibrary.create_connector", side_effect=AssertionError("Offline generation opened a DB")):
                        ingest.generate_embedding_csv_from_image_csv(str(root / "images.csv"), "Vectors",
                                                                     user_transformer=["fake_embedding"], class_name="Photos", hello_version=version)
                    csv_path = root / "images.csv_fake_embedding_connection.adb.csv"
                    with csv_path.open() as source:
                        rows = list(csv.DictReader(source))
                    expected = ("image:Photos@adb_image_sha256", "descriptor:Vectors@id") if version == 2 else (
                        "_Image@adb_image_sha256", "_Descriptor@id")
                    self.assertEqual(
                        list(rows[0]), ["ConnectionClass", *expected])
                    values = np.load(root / "images.csv_fake_embedding.npy")
                    np.testing.assert_array_equal(values, [[1., 2.]])
                    if version == 2:
                        commands, _ = ConnectionDataCSV(
                            str(csv_path), hello_version=2)[0]
                        self.assertEqual(
                            commands[0]["FindImage"]["with_class"], "Photos")
                        self.assertEqual(
                            commands[1]["FindDescriptor"]["with_class"], "Vectors")
        finally:
            os.chdir(original)

    def test_csv_generation_rejects_missing_configuration_before_loading_data(self):
        for options in [{}, {"hello_version": 3}, {"hello_version": 2, "user_transformer": ["fake"]}]:
            with self.subTest(options=options), self.assertRaises(ValueError):
                ingest.generate_embedding_csv_from_image_csv(
                    "missing.csv", "Vectors", **options)
