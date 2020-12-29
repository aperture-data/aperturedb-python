import os

from aperturedb import Image

import torch
from torch.utils import data
from torchvision import transforms

class ApertureDBDataset(data.Dataset):

    # initialise function of class
    def __init__(self, db, constraints):

        self.imgs_handler = Image.Images(db)
        self.imgs_handler.search(constraints=constraints, limit=50)

    # obtain the sample with the given index
    def __getitem__(self, index):

        img   = self.imgs_handler.get_np_image_by_index(index)
        label = self.imgs_handler.get_bboxes_by_index(index)

        img = transforms.ToTensor()(img)
        # label = torch.as_tensor(label, dtype=torch.int64)

        return img, label

    # the total number of samples (optional)
    def __len__(self):
        return self.imgs_handler.total_results()
