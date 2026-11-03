"""
Descriptor engine handling shared by the SDK helpers.

Servers from the updated 0.20.0 accept the 1.0.0 ``engines`` configuration
object and count the older ``engine`` field and implicit search engines as
deprecated usage. These helpers send the current forms when the server accepts
them and keep the older forms for earlier servers.
"""
from __future__ import annotations

import logging
from typing import Iterable, Optional, Union

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

    An empty selection maps to IVF, the engine earlier releases chose implicitly.
    """
    names = [engine] if isinstance(engine, str) else list(engine or [])
    if not names:
        names = ["IVF"]
    return {_KEYS.get(name, name.lower()): {} for name in names}


def select_engine(configured) -> Optional[str]:
    """
    The engine a search uses by default, from either form of ``_engines``:
    the 0.20.0 list of names or the 1.0.0 configuration object.
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
        response, _ = client.query(
            [{"FindDescriptorSet": {"with_name": set_name, "engines": True}}])
        entities = response[0]["FindDescriptorSet"].get("entities") or []
    except Exception as e:
        logger.debug(f"Cannot read engines of descriptor set {set_name}: {e}")
        return None
    return select_engine(entities[0].get("_engines")) if entities else None


def accepts_engines_object(client) -> bool:
    """
    Whether ``AddDescriptorSet`` accepts the ``engines`` object, according to
    ``GetApiSchema``. The answer is kept on the connector for later calls.
    """
    cached = getattr(client, "_accepts_engines_object", None)
    if cached is not None:
        return cached
    accepted = False
    try:
        response, _ = client.query([{"GetApiSchema": {}}])
        commands = response[0]["GetApiSchema"]["schema"]["items"]["properties"]
        accepted = "engines" in commands["AddDescriptorSet"]["properties"]
    except Exception as e:
        logger.debug(f"Cannot read the API schema: {e}")
    try:
        client._accepts_engines_object = accepted
    except AttributeError:
        pass
    return accepted
