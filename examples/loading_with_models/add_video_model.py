from aperturedb.QueryTypes import VideoModel, ClipModel, DescriptorModel, DescriptorSetModel
from aperturedb.Utils import create_connector
from aperturedb.ParallelQuery import execute_batch
from aperturedb.Query import generate_save_query
from aperturedb.Query import RangeType


class ClipEmbeddingModel(ClipModel):
    embedding: DescriptorModel


class VideoClipsModel(VideoModel):
    title: str
    description: str
    clips: list[ClipEmbeddingModel] = []


def save_video_details_to_aperturedb(URL: str, embeddings):
    video = VideoClipsModel(url=URL, title="Title", description="Description")
    for embedding in embeddings:
        video.clips.append(ClipEmbeddingModel(
            range_type=RangeType.TIME,
            start=embedding['start_offset_sec'],
            stop=embedding['end_offset_sec'],
            embedding=DescriptorModel(
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
q, blobs, c = generate_save_query(descriptorset)
result, response, blobs = execute_batch(q=q, blobs=blobs, db=db)
print(f"{result=}, {response=}")

# Create a video object, with clips, and embeddings
video = save_video_details_to_aperturedb(video_url, embeddings)
q, blobs, c = generate_save_query(video)
result, response, blobs = execute_batch(q=q, blobs=blobs, db=db)
print(f"{result=}, {response=}")
