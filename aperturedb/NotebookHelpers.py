import os
import cv2
import numpy as np

from PIL import Image
from IPython.display import display as ds

DESTINATION_FOLDER = "result_images"

def check_folder(folder):
    if not os.path.exists(folder):
            os.makedirs(folder)

def display(images_array, save=False):

    for im in images_array:
        nparr = np.fromstring(im, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        ds(Image.fromarray(image))

    if save:
        check_folder(DESTINATION_FOLDER)
        counter = 0
        for im in images_array:
            img_file = DESTINATION_FOLDER + '/res_' + str(counter) + '.jpg'
            counter += 1
            fd = open(img_file, 'wb')
            fd.write(im)
            fd.close()

def draw_bboxes(image, boxes=[], tags=[], save=False):

    nparr    = np.fromstring(image, np.uint8)
    cv_image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

    # Draw a rectangle around the faces
    counter = 0
    for coords in boxes:
        left   = coords["x"]
        top    = coords["y"]
        right  = coords["x"] + coords["width"]
        bottom = coords["y"] + coords["height"]
        cv2.rectangle(cv_image, (left, top), (right, bottom), (0, 255, 0), 2)
        y = top - 15 if top - 15 > 15 else top + 15
        cv2.putText(cv_image, tags[counter], (left, y), cv2.FONT_HERSHEY_SIMPLEX, 0.75, (0, 255, 0), 2)
        counter += 1

    cv_image_rgb = cv2.cvtColor(cv_image, cv2.COLOR_BGR2RGB)
    ds(Image.fromarray(cv_image_rgb))

    if save:
        check_folder(DESTINATION_FOLDER)
        img_file = DESTINATION_FOLDER + '/res_bboxes.jpg'
        cv2.imwrite(img_file, cv_image)
