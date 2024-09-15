from aperturedb.DataModels import VideoDataModel, ClipDataModel, DescriptorDataModel, DescriptorSetDataModel
from aperturedb.CommonLibrary import create_connector, execute_query
from aperturedb.Query import generate_add_query
from aperturedb.Query import RangeType
from aperturedb.Descriptors import Descriptors
from typing import List
import json

# Define the models for the associstaion of Video, Video Clips, and Embeddings
# Note : Video has multiple Clips, and each Clip has an embedding.

# Video clip -> Embedding.


class ClipEmbeddingModel(ClipDataModel):
    embedding: DescriptorDataModel

# Video -> Video Clips


class VideoClipsModel(VideoDataModel):
    title: str
    description: str
    clips: List[ClipEmbeddingModel] = []

# Function to create a connected Video object model.


def save_video_details_to_aperturedb(URL: str, embeddings):
    video = VideoClipsModel(url=URL, title="Title", description="Description")
    # Use the embeddings to create the video clips, and add them to the video object
    for embedding in embeddings:
        video.clips.append(ClipEmbeddingModel(
            range_type=RangeType.TIME,
            start=embedding['start_offset_sec'],
            stop=embedding['end_offset_sec'],
            embedding=DescriptorSetDataModel(
                # The corresponding descriptor to the Video Clip.
                vector=embedding['embedding'], set=descriptorset)
        ))
    return video


video_url = "https://storage.googleapis.com/ad-demos-datasets/videos/Ecommerce%20v2.5.mp4"
embeddings = None
with open("embeddings.json", "r") as f:
    embeddings = json.load(f)

client = create_connector()

# Create a descriptor set
descriptorset = DescriptorSetDataModel(name="marengo26", dimensions=3)
q, blobs, c = generate_add_query(descriptorset)
result, response, blobs = execute_query(query=q, blobs=blobs, client=client)
print(f"{result=}, {response=}")

# Create a video object, with clips, and embeddings
video = save_video_details_to_aperturedb(video_url, embeddings)
q, blobs, c = generate_add_query(video)
result, response, blobs = execute_query(query=q, blobs=blobs, client=client)
print(f"{result=}, {response=}")
