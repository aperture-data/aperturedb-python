import os
from typing import List
import cv2
import numpy as np

from PIL import Image
from IPython.display import Video
from IPython.display import display as ds
from IPython.display import HTML
from base64 import b64encode


DESTINATION_FOLDER = "results"
BOX_TEMPORAL_WINDOW = 10


class Rectangle:
    def __init__(self, x: int = 0, y: int = 0, width: int = 0, height: int = 0):
        self.x = x
        self.y = y
        self.width = width
        self.height = height


class BoundingBox:
    def __init__(self, r: Rectangle = None, label: str = None) -> None:
        self.r = r
        self.label = label


class TemporalBoundingBox:
    def __init__(self, bb: BoundingBox = None, start_frame: int = 0, end_frame = 0) -> None:
        self.bb = bb
        self.start_frame = start_frame
        self.end_frame = end_frame


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
        cv2.putText(cv_image, tags[counter], (left, y),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.75, (0, 255, 0), 2)
        counter += 1

    cv_image_rgb = cv2.cvtColor(cv_image, cv2.COLOR_BGR2RGB)
    ds(Image.fromarray(cv_image_rgb))

    if save:
        check_folder(DESTINATION_FOLDER)
        img_file = DESTINATION_FOLDER + '/res_bboxes.jpg'
        cv2.imwrite(img_file, cv_image)


def save_video(blob, name):
    check_folder(DESTINATION_FOLDER)
    with open(name, 'wb') as fd:
        fd.write(blob)


def display_video_mp4(blob):
    name = DESTINATION_FOLDER + "/" + "video_tmp.mp4"
    save_video(blob, name)
    ds(Video(name, embed=True))


def annotate_video(blob, boxes=[], tags=[], bboxes: List[TemporalBoundingBox] = []):
    name = DESTINATION_FOLDER + "/" + "video_tmp.mp4"
    save_video(blob, name)
    cap = cv2.VideoCapture(name)
    if cap.isOpened():
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        res = (int(width), int(height))
        fourcc = cv2.VideoWriter_fourcc(*'avc1')  # codec

        out = cv2.VideoWriter(os.path.join(
            DESTINATION_FOLDER, 'output.mp4'), fourcc, 20.0, res)
        frame = None
        count = 0
        while True:
            try:
                is_success, frame = cap.read()
                filtered = list(filter(lambda x: abs(
                    x.start_frame - count) < BOX_TEMPORAL_WINDOW or
                    abs(x.end_frame - count) < BOX_TEMPORAL_WINDOW, bboxes))
                if len(filtered) > 0:
                    start = (filtered[0].bb.r.x, filtered[0].bb.r.y)
                    end = (filtered[0].bb.r.x + filtered[0].bb.r.width,
                           filtered[0].bb.r.y + filtered[0].bb.r.height)
                    color = (255, 255, 255)
                    thickness = 2
                    annotated = cv2.rectangle(
                        frame, start, end, color, thickness)
                else:
                    annotated = frame
            except cv2.error as e:
                print(e)
                break

            if not is_success:
                break

            out.write(annotated)
            count += 1

        out.release()
    else:
        print("Unable to open cap")


def display_annotated_video(blob, boxes=[], tags=[], bboxes: List[TemporalBoundingBox] = []):
    """
    Returns a HTML representation with a column filled
    with video entities.
    """
    annotate_video(blob, boxes, tags, bboxes)
    video_path = os.path.join(DESTINATION_FOLDER, 'output.mp4')
    mp4 = open(video_path, "rb").read()
    data_url = "data:video/mp4;base64," + b64encode(mp4).decode()
    return f"""<div style='max-width: 100%; overflow: auto;'><video width=400 controls>
        <source src="{data_url}" type="video/mp4">
    </video></div>"""
