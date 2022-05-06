# Code examples with aperturedb.

Following are the table of contents for this folder and it's subfolders.
There are instructions to run these scripts also.

A part of Coco validation needs to be downloaded. 
This is a prerequisite for running some of the scripts below.

```
mkdir coco && cd coco && wget http://images.cocodataset.org/zips/val2017.zip && unzip val2017.zip && wget http://images.cocodataset.org/annotations/annotations_trainval2017.zip && unzip annotations_trainval2017.zip
```

## Example 1: ApertureDB Loaders 101

The following files are under *loaders_101*
| File | Description | instructions |
| -----| ------------| -----|
| loaders.ipynb | A notebook with some sample code for aperturedb | Also available to read at [Aperturedb python documentation](https://python.docs.aperturedata.io/examples/loaders.html)|

## Example 2: Image classification using a pretrained model
The following files are under *image_classification*

| File | Description | instructions |
| -----| ------------| -----|
| AlexNetClassifier.py | Helper code to transorm images before using pretrained alexnet model to classify them | Is not invoked directly |
| imagenet_classes.txt | The class labels for the outputs from alexnet | used by pytorch_classification.py |
| prepare_aperturedb.py | Helper to download images from coco dataset, and load them into aperturedb | ``python prepare_aperturedb.py -images_count 100`` |
| pytorch_classification.py | Pulls all images from aperturedb with a certain property set by prepare_aperturedb.py script , and classifies them using alexnet | ``python pytorch_classification.py`` |
| pytorch_classification.ipynb | It does the same operation as ``pytorch_classification.py``. Also displays the classified images | Also available to read at [Aperturedb python documentation](https://python.docs.aperturedata.io/examples/pytorch_classification.html) |