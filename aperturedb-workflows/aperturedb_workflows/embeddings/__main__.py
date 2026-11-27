from . import Embedder, SUPPORTED_MODELS

# Invoke this module directly to warm the cache
if __name__ == "__main__":
    for spec in SUPPORTED_MODELS:
        print(f"\n=== Warming cache for {spec} ===")
        be = Embedder(**Embedder.parse_string(spec), device="cpu")
        be.summarize()
