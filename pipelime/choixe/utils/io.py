import json
from pathlib import Path
from typing import Any

import yaml


def load(path: Path) -> Any:
    """Loads an object from a file with a supported markup format.
    Supported formats include:
    - yaml
    - json

    Args:
        path (Path): Path to the file to load.

    Returns:
        Any: The loaded object.
    """
    with open(path, "r") as fd:
        ext = path.suffix
        if ext in (".yaml", ".yml"):
            return yaml.safe_load(fd)
        elif ext == ".json":
            return json.load(fd)


def dump(obj: Any, path: Path) -> None:
    """Dumps an object to a file with a supported markup format.
    Supported formats include:
    - yaml
    - json
    Args:
        obj (Any): The object to dump.
        path (Path): Path to the file to write.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as fd:
        ext = path.suffix
        if ext in (".yaml", ".yml"):
            yaml.safe_dump(obj, fd, sort_keys=False)
        elif ext == ".json":
            json.dump(obj, fd)
