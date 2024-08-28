# This file only exists to support readable type hints

from typing import List, Dict, Any

Command = Dict[str, Any]
Blob = bytes
Commands = List[Command]  # aka Query, but that's also a class name
Blobs = List[Blob]
CommandResponses = List[Dict]

Image = bytes
Video = bytes
Descriptor = bytes
