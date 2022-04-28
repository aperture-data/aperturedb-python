import torch
from torchvision import transforms
from torchvision import models
import numpy as np
import cv2
from PIL import Image


class AlexNetClassifier(object):

    def __init__(self):

        self.alexnet = models.alexnet(pretrained=True)

        self.transform = transforms.Compose([
            # transforms.Resize(256), # Resize done by ApertureDB
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(
                mean=[0.485, 0.456, 0.406],
                std =[0.229, 0.224, 0.225]
            )])

        with open('imagenet_classes.txt') as f:
            self.classes = [line.strip() for line in f.readlines()]

    def classify(self, image_blob):

        nparr = np.fromstring(image_blob, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        img   = Image.fromarray(image.astype('uint8'), 'RGB')

        img_t = self.transform(img)
        batch_t = torch.unsqueeze(img_t, 0)
        self.alexnet.eval()
        out = self.alexnet(batch_t)
        _, index = torch.max(out, 1)
        percentage = torch.nn.functional.softmax(out, dim=1)[0] * 100

        label = self.classes[index[0]]
        confidence = percentage[index[0]].item()

        return label, confidence

    def print_model(self):
        # dir(models)
        print(self.alexnet)
