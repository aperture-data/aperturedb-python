from facenet_pytorch import MTCNN, InceptionResnetV1
from PIL import Image
import torch

# If required, create a face detection pipeline using MTCNN:
mtcnn = MTCNN(image_size=96, margin=0)

# Create an inception resnet (in eval mode):
resnet = InceptionResnetV1(pretrained='vggface2').eval()

errors = 0


def generate_embedding(img):
    global errors
    # Get cropped and prewhitened image tensor
    img_cropped = mtcnn(img)
    if img_cropped is not None:
        # Calculate embedding (unsqueeze to add batch dimension)
        img_embedding = resnet(img_cropped.unsqueeze(0))
    else:
        img_embedding = torch.FloatTensor(1, 512)
        errors += 1

    return img_embedding
