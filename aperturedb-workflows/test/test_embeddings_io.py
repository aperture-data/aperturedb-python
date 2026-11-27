"""Vector index metadata helpers, without models or a database."""
import importlib.util
from importlib.resources import files
from pathlib import Path
from unittest.mock import Mock, patch


def load_vector_io():
    # Load the file directly: the embeddings package imports torch.
    path = Path(str(files("aperturedb_workflows"))) / \
        "embeddings/aperturedb_io.py"
    spec = importlib.util.spec_from_file_location("vector_io", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_embedding_metadata_updates_only_the_selected_kind():
    module = load_vector_io()
    client = Mock()
    indexes = [{"kind": "flat", "params": {"metric": ["IP"]}, "metadata": {"tag": "exact"}},
               {"kind": "hnsw", "params": {"metric": ["L2"]}, "metadata": {"tag": "graph"}}]
    with patch.object(module, "Utils") as utils, patch.object(module, "_execute") as execute:
        utils.return_value.get_vector_indexes.return_value = indexes
        assert module.find_descriptor_set(client, "Images")["tag"] == "exact"
        module.update_descriptor_set(
            client, "Images", {"model": "example"}, engine="HNSW")
        update = execute.call_args.args[1][0]["UpdateIndex"]
        assert update["kind"] == "hnsw"
        assert update["metadata"] == {"tag": "graph", "model": "example"}
