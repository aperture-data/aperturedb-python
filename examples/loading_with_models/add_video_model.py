from typing import List
from aperturedb.DataModels import VideoDataModel, ClipDataModel, DescriptorDataModel, DescriptorSetDataModel
from aperturedb.CommonLibrary import create_connector, execute_query
from aperturedb.Query import generate_add_query
from aperturedb.Query import RangeType
import json

# In aperturedb we have Videos, Video Clips and Embeddings(aka Descriptors)
#recognized as first class objects.
# Note : Video has multiple Clips, and each Clip has an embedding.

# In aperturedb.datamodel, we already define datamodels for Videos, Clips and Embeddings
# Now Define the data models for the "association" of Video, Video Clips, and Embeddings

# Video clip -> Embedding.


class ClipEmbeddingModel(ClipDataModel):
    embedding: DescriptorDataModel

# Video -> Video Clips


class VideoClipsModel(VideoDataModel):
    title: str
    description: str
    clips: List[ClipEmbeddingModel] = []


# Function to create a connected Video object model.
def save_video_details_to_aperturedb(URL: str, clips, collection):
    video = VideoClipsModel(url=URL, title="Ecommerce v2.5",
                            description="Ecommerce v2.5 video with clips by Marengo26")
    # Use the embeddings to create the video clips, and add them to the video object
    for clip in clips:
        video.clips.append(ClipEmbeddingModel(
            range_type=RangeType.TIME,
            start=clip['start_offset_sec'],
            stop=clip['end_offset_sec'],
            embedding=DescriptorDataModel(
                # The corresponding descriptor to the Video Clip.
                vector=clip['embedding'], set=collection)
        ))
    return video


video_url = "https://storage.googleapis.com/ad-demos-datasets/videos/Ecommerce%20v2.5.mp4"

clips = None
with open("video_clips.json", "r") as f:
    clips = json.load(f)

client = create_connector()

# Create a descriptor set
# DS is a search space for descriptors added to it (some times called collections)
# https://docs.aperturedata.io/HowToGuides/Advanced/similarity_search#descriptorsets-and-descriptors
collection = DescriptorSetDataModel(
    name="marengo26", dimensions=len(clips[0]['embedding']))
q, blobs, c = generate_add_query(collection)
result, response, blobs = execute_query(query=q, blobs=blobs, client=client)
print(f"{result=}, {response=}")

# Create a video object, with clips, and embeddings
video = save_video_details_to_aperturedb(video_url, clips, collection)
q, blobs, c = generate_add_query(video)
result, response, blobs = execute_query(query=q, blobs=blobs, client=client)
print(f"{result=}, {response=}")
