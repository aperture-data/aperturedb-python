"""
Descriptor engine handling shared by the SDK helpers.

Each native descriptor index has one engine kind.
"""
from __future__ import annotations

import logging
from typing import Iterable, Optional, Union
from aperturedb.ServerCompatibility import typed_server, vector_index

logger = logging.getLogger(__name__)

# A search that names no engine uses the first configured engine in this order.
ENGINE_ORDER = (("flat", "Flat"), ("ivf", "IVF"),
                ("hnsw", "HNSW"), ("acorn", "ACORN"))
_KEYS = {name: key for key, name in ENGINE_ORDER}
_KEYS.update({"FaissFlat": "flat", "FaissIVFFlat": "ivf",
              "FaissHNSWFlat": "hnsw"})


def engines_object(engine: Union[str, Iterable[str], None]) -> dict:
    """
    The ``engines`` configuration object for engine names, with default settings.

    An empty selection maps to IVF, the default engine.
    """
    names = [engine] if isinstance(engine, str) else list(engine or [])
    if not names:
        names = ["IVF"]
    return {_KEYS.get(name, name.lower()): {} for name in names}


def select_engine(configured) -> Optional[str]:
    """
    Select the default search engine from engine names or a settings mapping.
    """
    if isinstance(configured, (list, tuple)):
        keys = {_KEYS.get(name) for name in configured}
        return next((name for key, name in ENGINE_ORDER if key in keys), None)
    if isinstance(configured, dict):
        return next((name for key, name in ENGINE_ORDER
                     if isinstance(configured.get(key), dict)), None)
    return None


def descriptorset_engine(client, set_name: str) -> Optional[str]:
    """
    The configured engine a search of ``set_name`` would use by default, or None
    when the set cannot be read. Searches then omit ``engine`` and report the
    server's own error.
    """
    try:
        if typed_server(client):
            return dict(ENGINE_ORDER)[vector_index(client, set_name)["kind"]]
        response, _ = client.query(
            [{"FindDescriptorSet": {"with_name": set_name, "engines": True}}])
        entities = response[0]["FindDescriptorSet"].get("entities") or []
    except Exception as e:
        logger.debug(f"Cannot read engines of descriptor set {set_name}: {e}")
        return None
    return select_engine(entities[0].get("_engines")) if entities else None


def descriptor_search(client, class_name, engine=None, metric=None):
    """Build a scoped search selector for either supported server API."""
    if not typed_server(client):
        selected = engine or descriptorset_engine(client, class_name)
        return {"set": class_name, **({"engine": selected} if selected else {}),
                **({"metric": metric} if metric else {})}
    definition = vector_index(client, class_name, engine, metric)
    selected = dict(ENGINE_ORDER)[definition["kind"]]
    metrics = definition["params"]["metric"]
    if isinstance(metrics, str):
        metrics = [metrics]
    return {"with_class": class_name, "engine": selected,
            "metric": metric or metrics[0]}
