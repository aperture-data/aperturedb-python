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

## Example 3: Similarity search using apertureDB

This needs a bit of extra setup.
- Install the dependent packages using the commands as shown, in the top level path of this repo.
```
pip install ".[complete]"

```
- Setup kaggle account and the API token as per the official [kagggle api guide](https://github.com/Kaggle/kaggle-api).

The following files are under *similarity_search*

| File | Description | instructions |
| -----| ------------| -----|
| similarity_search.ipynb | A notebook with some sample code for describing similarity search using aperturedb | Also available to read at [Aperturedb python documentation](https://python.docs.aperturedata.io/examples/loaders.html)|
| facenet.py | Face Recognition using facenet and pytorch | Is invoked indirectly |
| add_faces.py | A Script to load celebA dataset into aperturedb | ``python add_faces.py``|

## Example 4: REST interface to apertureDB.

The following files are under *rest_api*

| File | Description | instructions |
| -----| ------------| -----|
| rest_api.py | Interactions with aperturedb using python's requests | ``python rest_api.py``|
| rest_api.js | Interactions with aperturedb using javascript with axios | Is included in index.html |
| index.html | A web page that renders from responses from aperturedb | Tested on chrome |

## Example 5: Adding Data to aperturedb with User defined models

The following files are under *loading_with_models*

| File | Description | instructions |
| -----| ------------| -----|
| models.ipynb | A notebook with some sample code to add data using models | Also available to read at [Aperturedb model exmaple](https://python.docs.aperturedata.io/examples/loading.html)|
