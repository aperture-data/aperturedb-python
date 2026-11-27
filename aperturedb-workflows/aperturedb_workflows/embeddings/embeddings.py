import torch
import numpy as np
import hashlib
from typing import List, Union, Literal, Optional
import cv2
from PIL import Image
import logging
from aperturedb.Connector import Connector
import inspect

# Set up logging
logger = logging.getLogger(__name__)

# provider model pretrained
# Keep this short to reduce docker build time and image size
SUPPORTED_MODELS = [
    "sentence-transformers all-MiniLM-L6-v2 Qwen/Qwen3-Embedding-4B",
    "gpt4all all-MiniLM-L6-v2.gguf2.f16.gguf",
    "openclip ViT-B-32 laion2b_s34b_b79k",
    "openclip ViT-L-14 laion2b_s32b_b82k",
    "openclip RN50 yfcc15m",
    "clip ViT-B/32 openai"
]

DEFAULT_MODEL = SUPPORTED_MODELS[0]

# Do not change this text
# It is used to generate a fingerprint for the model
FINGERPRINT_TEXT = "ApertureDB unifies multimodal data, knowledge graphs, and vector search into a single database solution for rapid AI deployments at enterprise scale."


class DescriptorSetNotFoundError(Exception):
    """Exception raised when a descriptor set is not found."""

    def __init__(self, descriptor_set: str):
        super().__init__(f"Descriptor set '{descriptor_set}' not found.")
        self.descriptor_set = descriptor_set


class Embedder():
    """
    This class is a general helper for embedding text and images using OpenCLIP or CLIP models.

    The recommended way to construct an Embedder is to use one of the following factory methods:
    * `from_existing_descriptor_set`: This will find an existing descriptor set in the database and create an Embedder instance from it. Only the descriptor set name is required. This method is recomended for read-only use cases where you want to embed query texts or images to search for with an existing descriptor set.
    * `from_new_descriptor_set`: This will find an existing descriptor set in the database or create a new one if it does not exist. You must provide the provider, model name, pretrained corpus, and descriptor set name. This method is recommended for write use cases where you want to create a new descriptor set with embeddings for texts or images, but you also want to support extention of an existing descriptor set if it already exists. See the `clean` parameter for details of how existing descriptor sets are handled.

    These two methods are the only ones to talk to the database, and do not store the client.

    In addition the `parse_string` static method can be used to parse a string containing the provider, model name, and optionally the pretrained corpus, e.g., "openclip ViT-B-32 laion2b_s34b_b79k". This is offered as a convenience to avoid specifying each parameter separately, for example when configuring the model from an environment variable.

    Once you have an Embedder instance, you can use it to embed texts and images:
    * `embed_text`: Embed a single text input.
    * `embed_texts`: Embed a list of text inputs.
    * `embed_image`: Embed a single image input.
    * `embed_images`: Embed a list of images.

    All of these methods return a the embedded vectors as Numpy arrays. To use this as a blob in an ApertureDB query call `tobytes()`.
    """
    supported_providers = ["clip", "openclip",
                           "gpt4all", "sentence-transformers"]

    def __init__(self,
                 provider: Literal["clip", "openclip",
                                   "gpt4all", "sentence-transformers"] = None,
                 model_name: str = None,
                 pretrained: str = None,
                 descriptor_set: str = None,
                 device: Optional[Literal["cpu", "cuda"]] = None):
        """Initialize the Embedder with a model specification.

        Args:
            provider (str): The provider of the model, e.g., "openclip" or "clip".
            model_name (str): The name of the model, e.g., "ViT-B-32".
            pretrained (str): The pretrained corpus, e.g., "laion2b_s34b_b79k". (Optional for CLIP)
            descriptor_set (str): The name of the descriptor set to use for this embedder.
            device (str): The device to run the model on. Default is to auto-detect.
        """
        assert provider in self.supported_providers, \
            f"Unsupported provider: {provider}. {self.supported_providers=}"
        self.provider = provider

        assert model_name, "Model name must be specified."
        self.model_name = model_name

        if not pretrained and provider == "clip":
            pretrained = "openai"
        if not pretrained and provider in ["gpt4all", "sentence-transformers"]:
            pretrained = "figure_me_out"
        assert pretrained, "Pretrained corpus must be specified for OpenCLIP."
        self.pretrained = pretrained

        self.descriptor_set = descriptor_set

        # This should be "cpu" during docker build
        device_name = device or (
            "cuda" if torch.cuda.is_available() else "cpu")
        self.device = torch.device(device_name)

        # https://github.com/nomic-ai/gpt4all/tree/main/gpt4all-bindings/python
        self.gpt4all_device_name = "gpu" if device_name == "cuda" else "intel"

        self._load_model()  # sets self.preprocess, self.tokenizer

    @staticmethod
    def parse_string(provider_model_pretrained: str) -> dict:
        """Parse a string specification into Embedder parameters.

        Args:
            * provider_model_pretrained: A string containing the provider, model name, and optionally the pretrained corpus, e.g., "openclip ViT-B-32 laion2b_s34b_b79k". This is offered as a convenience to avoid specifying each parameter separately, for example when configuring the model from an environment variable.

        Returns:
            kwargs: A dictionary containing the provider, model name, and pretrained corpus, suitable for dict interpolation into the Embedder constructor.
        """
        parts = provider_model_pretrained.split()
        if len(parts) < 2:
            raise ValueError(
                "Model specification must include provider and model name")
        provider = parts[0].lower()
        model_name = parts[1]
        pretrained = parts[2] if len(parts) > 2 else None
        return dict(provider=provider, model_name=model_name, pretrained=pretrained)

    def _load_model(self):
        """Load the model based on the provider, model name, and pretrained corpus."""
        # TODO: Consider caching this so that multiple instances of Embedder
        # with the same provider, model name, and pretrained corpus can share the same model.
        if self.provider == "openclip":
            import open_clip
            self.model, self.preprocess, _ = open_clip.create_model_and_transforms(
                model_name=self.model_name,
                pretrained=self.pretrained,
                device=self.device
            )
            self.model.eval()
            self.tokenizer = open_clip.get_tokenizer(self.model_name)
            self.context_length = inspect.signature(
                self.tokenizer.__call__).parameters["context_length"].default

        elif self.provider == "clip":
            import clip
            model_id = self.model_name
            # CLIP assumes pretrained corpus is "openai"
            self.model, self.preprocess = clip.load(
                model_id, device=self.device)
            self.model.eval()
            self.tokenizer = lambda x: clip.tokenize(x, truncate=True)
            self.context_length = inspect.signature(
                clip.tokenize).parameters["context_length"].default
        elif self.provider == "gpt4all":
            from gpt4all import Embed4All
            self.model = Embed4All(model_name=self.model_name)
        elif self.provider == "sentence-transformers":
            from sentence_transformers import SentenceTransformer
            self.model = SentenceTransformer(self.model_name)
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")

    def _tokenize(self, texts: List[str]) -> torch.Tensor:
        """Tokenizes the input texts using the model's tokenizer."""
        if self.tokenizer is None:
            raise ValueError(
                "Tokenizer is not initialized. Call _load_model() first.")
        return self.tokenizer(texts).to(self.device)

    def get_properties(self) -> dict:
        """Returns properties that might be added to, say, a descriptor set."""
        return {
            "embeddings": f"{self.provider} {self.model_name} {self.pretrained}",
            "embeddings_provider": self.provider,
            "embeddings_model": self.model_name,
            "embeddings_pretrained": self.pretrained,
            "embeddings_fingerprint": self.fingerprint_hash(),
        }

    @classmethod
    def check_properties(cls, properties: dict) -> bool:
        """
        Check if the properties are valid for this embedder.
        This attempts to verify that the properties contain the required keys for an embedder.

        Args:
            properties (dict): The properties to check, stored in the vector index metadata.

        Returns:
            bool: True if the properties are valid for this embedder, False otherwise.
        """
        required_keys = ["embeddings_provider",
                         "embeddings_model", "embeddings_pretrained"]
        # TODO: Consider adding more checks, e.g., for the provider and model name.
        return all(key in properties and properties[key] is not None
                   for key in required_keys)

    @classmethod
    def from_properties(cls,
                        properties: dict,
                        descriptor_set: str,
                        device: Optional[Literal["cpu", "cuda"]] = None) -> "Embedder":
        """Create an Embedder instance from properties."""
        provider = properties.get("embeddings_provider")
        model_name = properties.get("embeddings_model")
        pretrained = properties.get("embeddings_pretrained")

        if not provider or not model_name or not pretrained:
            raise ValueError(
                f"Properties must contain 'embeddings_provider', 'embeddings_model', and 'embeddings_pretrained': {descriptor_set} - {properties}.")

        return cls(provider=provider,
                   model_name=model_name,
                   pretrained=pretrained,
                   descriptor_set=descriptor_set,
                   device=device)

    @classmethod
    def from_new_descriptor_set(cls,
                                client: Connector,
                                descriptor_set: str,
                                provider: str = None,
                                model_name: str = None,
                                pretrained: str = None,
                                engine: str = "HNSW",
                                device: Optional[Literal["cpu",
                                                         "cuda"]] = None,
                                clean: bool = False,
                                properties: Optional[dict] = None
                                ) -> "Embedder":
        """Find or create a descriptor set for the embedder.

        Args:
            client (Connector): The ApertureDB client to use.
            descriptor_set (str): The name of the descriptor set to use.
            provider (str): The provider of the model, e.g., "openclip" or "clip".
            model_name (str): The name of the model, e.g., "ViT-B-32".
            pretrained (str): The pretrained corpus, e.g., "laion2b_s34b_b79k". (Optional for CLIP)
            device (str): The device to run the model on. Default is to auto-detect.
            clean (bool): If True, delete the existing descriptor set before creating a new one. If the descriptor set already exists, and the embedder is compatible, it will be extended with new embeddings. If it is not compatible, an error will be raised. Default is False.
        """
        logger.info(
            f"Creating Embedder for descriptor set '{descriptor_set}' with provider '{provider}', model '{model_name}', pretrained '{pretrained}', engine '{engine}', device '{device}, clean={clean}'")

        from .aperturedb_io import find_descriptor_set, add_descriptor_set, delete_descriptor_set, update_descriptor_set

        existing_properties = None

        if clean:
            delete_descriptor_set(client, descriptor_set)
        else:
            existing_properties = find_descriptor_set(
                client, descriptor_set, engine)
        needs_index = existing_properties is None
        if needs_index and not clean:
            # All index kinds on a class search the same stored embeddings.
            existing_properties = find_descriptor_set(client, descriptor_set)
        if existing_properties:
            logger.info(
                f"Found existing descriptor set {descriptor_set}: {existing_properties}")

            update_required = False

            # Verify that the existing descriptor set matches the requested parameters
            # We allow None values of the existing properties to allow for re-use of descriptor sets that were created with an earlier version of the code.
            if provider != existing_properties['embeddings_provider']:
                if existing_properties['embeddings_provider'] is not None:
                    raise ValueError(
                        f"Provider mismatch: {provider} != {existing_properties['embeddings_provider']} in descriptor set {descriptor_set}, properties: {existing_properties}")
                else:
                    logger.warning(
                        f"Provider mismatch: {provider} != {existing_properties['embeddings_provider']} in descriptor set {descriptor_set}, properties: {existing_properties}. Allowing re-use of descriptor set and updating the properties.")
                    update_required = True

            if model_name != existing_properties['embeddings_model']:
                if existing_properties['embeddings_model'] is not None:
                    raise ValueError(
                        f"Model name mismatch: {model_name} != {existing_properties['embeddings_model']} in descriptor set {descriptor_set}, properties: {existing_properties}")
                else:
                    logger.warning(
                        f"Model name mismatch: {model_name} != {existing_properties['embeddings_model']} in descriptor set {descriptor_set}, properties: {existing_properties}. Allowing re-use of descriptor set and updating the properties.")
                    update_required = True

            if pretrained and pretrained != existing_properties['embeddings_pretrained']:
                if existing_properties['embeddings_pretrained'] is not None:
                    raise ValueError(
                        f"Pretrained corpus mismatch: {pretrained} != {existing_properties['embeddings_pretrained']} in descriptor set {descriptor_set}, properties: {existing_properties}")
                else:
                    logger.warning(
                        f"Pretrained corpus mismatch: {pretrained} != {existing_properties['embeddings_pretrained']} in descriptor set {descriptor_set}, properties: {existing_properties}. Allowing re-use of descriptor set and updating the properties.")
                    update_required = True

            if update_required and not needs_index:
                properties = {
                    "embeddings_provider": provider,
                    "embeddings_model": model_name,
                    **({"embeddings_pretrained": pretrained} if pretrained else {}),
                }
                logger.info(
                    f"Updating descriptor set {descriptor_set} with properties: {properties}")
                update_descriptor_set(
                    client=client,
                    descriptor_set=descriptor_set,
                    properties=properties, engine=engine)
            else:
                logger.info(
                    f"Descriptor set {descriptor_set} already exists and is compatible with the requested properties.")
        else:
            logger.info(
                f"Descriptor set {descriptor_set} not found. Will create a new one.")

        self = cls(provider=provider,
                   model_name=model_name,
                   pretrained=pretrained,
                   descriptor_set=descriptor_set,
                   device=device)
        if needs_index:
            properties = {**(existing_properties or {}), **(properties or {})}
            properties = {k: v for k, v in properties.items() if v is not None}
            properties.update(self.get_properties())
            add_descriptor_set(
                client=client,
                descriptor_set=descriptor_set,
                metric=self.metric,
                dimensions=self.dimensions,
                engine=engine,
                properties=properties
            )
            logger.info(
                f"Created new descriptor set: {descriptor_set} with properties: {properties}")

        return self

    @classmethod
    def from_existing_descriptor_set(cls,
                                     client: Connector,
                                     descriptor_set: str,
                                     device: str = None,
                                     ) -> "Embedder":
        """Create an instance from a descriptor set name."""
        logger.info(
            f"Creating Embedder from existing descriptor set '{descriptor_set}' on device '{device}'")

        from .aperturedb_io import find_descriptor_set

        properties = find_descriptor_set(client, descriptor_set)
        if not properties:
            raise ValueError(
                f"Descriptor set '{descriptor_set}' not found.")

        self = cls.from_properties(properties=properties,
                                   descriptor_set=descriptor_set,
                                   device=device)

        logger.info(
            f"Created Embedder from descriptor set {descriptor_set}: {self}")

        return self

    @property
    def model_spec(self) -> str:
        """Return a string representation of the model specification."""
        return f"{self.provider} {self.model_name} {self.pretrained}"

    @staticmethod
    def _as_numpy(tensor: torch.Tensor) -> np.ndarray:
        """Convert a PyTorch tensor to a NumPy array."""
        return tensor.float().cpu().numpy()

    def embed_text(self, text: str) -> np.ndarray:
        """Embed a single text input."""
        return self.embed_texts([text])[0]

    def embed_texts(self, texts: List[str]) -> List[np.ndarray]:
        logger.debug(f"Embedding {len(texts)} texts on {self.device}")
        if self.provider == "gpt4all":
            return [np.array(embedding, dtype=np.float32) for embedding in self.model.embed(texts)]
        elif self.provider == "sentence-transformers":
            # self.model.encode(texts) returns a (len(texts), embedding_dim) array
            embeddings = self.model.encode(texts)
            return [embeddings[i] for i in range(len(texts))]
        tokens = self._tokenize(texts)

        with torch.no_grad():
            features = self.model.encode_text(tokens)

        features = [self._as_numpy(f) for f in features]

        assert all(f.shape[0] == self.dimensions for f in features), (
            f"Expected all embeddings to have {self.dimensions} dimensions, "
            f"got {[f.shape[0] for f in features]}")

        return features

    def embed_image(self, b: bytes) -> np.ndarray:
        """Embed a single image input.

        Args:
            image (bytes): The image data in bytes format (JPEG/PNG).

        Returns:
            vector (Numpy array): The embedded vector for the image.
        """
        return self.embed_images([b])[0]

    def embed_images(self, images: List[bytes]) -> List[np.ndarray]:
        """Embed a list of images.

        Args:
            images (List[bytes]): A list of image data in bytes format (JPEG/PNG).

        Returns:
            List[np.ndarray]: A list of embedded vectors for the images.
        """

        logger.debug(f"Embedding {len(images)} images on {self.device}")

        preprocessed = []

        for i, b in enumerate(images):
            try:
                assert isinstance(b, bytes), \
                    f"Image {i} must be bytes, got {type(b)}"
                nparr = np.frombuffer(b, np.uint8)
                image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
                image = Image.fromarray(image)
                tensor = self.preprocess(image)  # shape [C, H, W]
                preprocessed.append(tensor)
            except Exception as e:
                raise ValueError(
                    f"Failed to preprocess image {i}: {e}. Ensure the image is valid and in a supported format.")

        # Stack and move to device
        batch = torch.stack(preprocessed, dim=0).to(
            self.device)  # shape [B, C, H, W]

        with torch.no_grad():
            features = self.model.encode_image(batch)  # shape [B, D]

        features = [self._as_numpy(f) for f in features]  # List[np.ndarray]

        assert all(f.shape[0] == self.dimensions for f in features), (
            f"Expected all embeddings to have {self.dimensions} dimensions, "
            f"got {[f.shape[0] for f in features]}")
        return features

    def fingerprint(self, canonical_text: str = FINGERPRINT_TEXT) -> np.ndarray:
        result = self.embed_text(canonical_text)
        return result

    def fingerprint_hash(self, canonical_text: str = FINGERPRINT_TEXT) -> str:
        vec = self.fingerprint(canonical_text)
        return hashlib.sha256(vec.tobytes()).hexdigest()

    @property
    def dimensions(self) -> int:
        if self.provider == "gpt4all":
            return np.array(self.model.embed(FINGERPRINT_TEXT), dtype=np.float32).shape[0]
        elif self.provider == "sentence-transformers":
            return self.model.get_sentence_embedding_dimension()
        return self.model.visual.output_dim if hasattr(self.model, 'visual') else self.model.output_dim

    def summarize(self, canonical_text: str = FINGERPRINT_TEXT):
        vec = self.fingerprint(canonical_text)
        print(f"[INFO] Provider: {self.provider}")
        print(f"[INFO] Model: {self.model_name}")
        print(f"[INFO] Pretrained: {self.pretrained}")
        print(f"[INFO] Device: {self.device}")
        print(f"[INFO] Embedding Dim: {self.dimensions}")
        print(
            f"[INFO] Fingerprint Hash: {self.fingerprint_hash(canonical_text)}")

    @property
    def metric(self):
        # For now we believe all OpenCLIP and CLIP models are cosine similarity
        return "CS"

    def __repr__(self):
        return f"<Embedder {self.provider} {self.model_name} ({self.pretrained} for {self.descriptor_set}) on {self.device}>"
