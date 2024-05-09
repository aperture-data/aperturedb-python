import logging

logger = logging.getLogger(__name__)

error_message = """
Facenet transformer requires facenet-pytorch and torch
Install with: pip install aperturedb[complete]
Alternatively, install with: "pip install facenet-pytorch torch" in the same
venv as aperturedb.
"""

try:
    from facenet_pytorch import MTCNN, InceptionResnetV1
    import torch
except ImportError:
    logger.critical(error_message)
    exit(1)

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

# If required, create a face detection pipeline using MTCNN:
mtcnn = MTCNN(image_size=96, margin=0, device=device)

# Create an inception resnet (in eval mode):
resnet = InceptionResnetV1(pretrained='vggface2', device=device).eval()

errors = 0


def generate_embedding(img):
    global errors
    # Get cropped and prewhitened image tensor
    img_cropped = mtcnn(img)
    if img_cropped is not None:
        # Calculate embedding (unsqueeze to add batch dimension)
        img_embedding = resnet(img_cropped.unsqueeze(0).to(device))
    else:
        img_embedding = torch.zeros(1, 512).to(device)
        errors += 1

    return img_embedding
