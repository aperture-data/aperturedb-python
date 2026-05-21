import unittest
from unittest.mock import MagicMock, patch
from aperturedb.transformers.image_properties import ImageProperties
from aperturedb.Subscriptable import Subscriptable
import io
from PIL import Image

class DummyData(Subscriptable):
    def __init__(self, transactions):
        self.transactions = transactions
    def __len__(self):
        return len(self.transactions)
    def getitem(self, idx):
        return self.transactions[idx]

class TestImageProperties(unittest.TestCase):
    @patch('aperturedb.transformers.transformer.Transformer.get_utils')
    def test_image_properties_removes_width_height(self, mock_get_utils):
        # Mock get_utils behavior
        mock_utils = MagicMock()
        mock_utils.get_indexed_props.return_value = ["adb_data_source"]
        mock_get_utils.return_value = mock_utils

        # Create a tiny valid image (10x20)
        img = Image.new('RGB', (10, 20), color='red')
        img_bytes = io.BytesIO()
        img.save(img_bytes, format='JPEG')
        img_data = img_bytes.getvalue()

        # Mock transaction with existing width and height
        transactions = [
            (
                [{"AddImage": {"properties": {"width": 100, "height": 200, "id": "test_id"}}}],
                [img_data]
            )
        ]
        
        dummy_data = DummyData(transactions)
        
        # Instantiate the transformer
        transformer = ImageProperties(dummy_data)
        # Manually set the indices that would normally be set by Transformer base class
        transformer._add_image_index = [0]
        
        # Run transformation
        result = transformer[0]
        
        props = result[0][0]["AddImage"]["properties"]
        
        # Check expected behavior
        self.assertNotIn("width", props)
        self.assertNotIn("height", props)
        self.assertEqual(props["adb_image_width"], 10)
        self.assertEqual(props["adb_image_height"], 20)
        self.assertEqual(props["adb_image_id"], "test_id")
        self.assertIn("adb_image_size", props)
        self.assertIn("adb_image_sha256", props)

if __name__ == '__main__':
    unittest.main()
