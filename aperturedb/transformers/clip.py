
import logging

logger = logging.getLogger(__name__)

error_message = """
CLIP transformer requires openai-clip and torch
Install with: pip install aperturedb[complete], followed by explicit install of CLIP.
Can be done with : "pip install openai-clip" in the same
venv as aperturedb.
"""

try:
    import numpy as np
    from PIL import Image
    import clip
    import torch
    import cv2
except ImportError as e:
    logger.critical(error_message)
    raise ImportError(error_message) from e

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
