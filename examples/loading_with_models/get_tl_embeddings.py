from twelvelabs import TwelveLabs
from twelvelabs.models.embed import EmbeddingsTask

# Initialize the Twelve Labs client
twelvelabs_client = TwelveLabs(api_key=TL_API_KEY)


def generate_embedding(video_url):
    # Create an embedding task
    task = twelvelabs_client.embed.task.create(
        engine_name="Marengo-retrieval-2.6",
        video_url=video_url
    )
    print(
        f"Created task: id={task.id} engine_name={task.engine_name} status={task.status}")

    # Define a callback function to monitor task progress
    def on_task_update(task: EmbeddingsTask):
        print(f"  Status={task.status}")

    # Wait for the task to complete
    status = task.wait_for_done(
        sleep_interval=2,
        callback=on_task_update
    )
    print(f"Embedding done: {status}")

    # Retrieve the task result
    task_result = twelvelabs_client.embed.task.retrieve(task.id)

    # Extract and return the embeddings
    embeddings = []
    for v in task_result.video_embeddings:
        embeddings.append({
            'embedding': v.embedding.float,
            'start_offset_sec': v.start_offset_sec,
            'end_offset_sec': v.end_offset_sec,
            'embedding_scope': v.embedding_scope
        })

    return embeddings, task_result


# Example usage
# video_url = "http://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerJoyrides.mp4"
video_url = "https://storage.googleapis.com/ad-demos-datasets/videos/Ecommerce%20v2.5.mp4"

# Generate embeddings for the video
embeddings, task_result = generate_embedding(video_url)

print(f"Generated {len(embeddings)} embeddings for the video")
for i, emb in enumerate(embeddings):
    print(f"Embedding {i+1}:")
    print(f"  Scope: {emb['embedding_scope']}")
    print(
        f"  Time range: {emb['start_offset_sec']} - {emb['end_offset_sec']} seconds")
    print(f"  Embedding vector (first 5 values): {emb['embedding'][:5]}")
    print()
