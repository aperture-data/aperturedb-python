from aperturedb.DataModels import VideoDataModel, ClipDataModel, DescriptorDataModel, DescriptorSetDataModel
from aperturedb.CommonLibrary import create_connector
from aperturedb.ParallelQuery import execute_batch
from aperturedb.Query import generate_add_query
from aperturedb.Query import RangeType
from typing import List

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


video_url = "http://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerJoyrides.mp4"
embeddings = [
    {
        "start_offset_sec": 0.0,
        "end_offset_sec": 6.0,
        "embedding": [0.1, 0.2, 0.3],
        "embedding_scope": "clip"
    },
    {
        "start_offset_sec": 6.0,
        "end_offset_sec": 12.0,
        "embedding": [0.4, 0.5, 0.6],
        "embedding_scope": "clip"
    },
    {
        "start_offset_sec": 12.0,
        "end_offset_sec": 15.0,
        "embedding": [0.4, 0.5, 0.6],
        "embedding_scope": "clip"
    }
]

db = create_connector()

# Create a descriptor set
descriptorset = DescriptorSetModel(name="marengo26", dimensions=3)
q, blobs, c = generate_add_query(descriptorset)
result, response, blobs = execute_batch(q=q, blobs=blobs, db=db)
print(f"{result=}, {response=}")

# Create a video object, with clips, and embeddings
video = save_video_details_to_aperturedb(video_url, embeddings)
q, blobs, c = generate_add_query(video)
result, response, blobs = execute_batch(q=q, blobs=blobs, db=db)
print(f"{result=}, {response=}")
