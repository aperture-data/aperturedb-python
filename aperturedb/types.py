# This file only exists to support readable type hints

from typing import List, Dict, Any, Tuple

Command = Dict[str, Any]
Blob = bytes
Query = List[Command]
Blobs = List[Blob]
CommandResponses = List[Dict]

Image = bytes
Video = bytes
Descriptor = bytes
