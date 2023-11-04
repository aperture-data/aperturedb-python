
import logging
import numpy as np
from PIL import Image

logger = logging.getLogger(__name__)

error_message = """
CLIP transformer requires git+https://github.com/openai/CLIP.git and torch
Install with: pip install aperturedb[complete], followed by explicit install of CLIP.
Can be done with : "pip install git+https://github.com/openai/CLIP.git" in the same
venv as aperturedb.
"""

try:
    import clip
    import torch
    import cv2
except ImportError:
    logger.critical(error_message)
    exit(1)

descriptor_set = "ViT-B/16"
device = "cuda" if torch.cuda.is_available() else "cpu"
model, preprocess = clip.load(descriptor_set, device=device)


def generate_embedding(blob):
    global errors

    nparr = np.fromstring(blob, np.uint8)
    image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
    image = preprocess(Image.fromarray(image)).unsqueeze(0).to(device)

    image_features = model.encode_image(image)
    embedding = None
    if device == "cuda":
        image_features = image_features.float()
        embedding = image_features.detach().cpu().numpy().tobytes()
    else:
        embedding = image_features.detach().numpy().tobytes()

    return embedding
