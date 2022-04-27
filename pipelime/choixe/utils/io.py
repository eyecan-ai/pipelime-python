import json
from pathlib import Path
from typing import Any

import yaml


def get_extension(path: Path) -> str:
    """Returns the extension of a file, given a path."""
    return path.name.split(".")[-1]


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
    ext = get_extension(path)
    if ext in ["yaml", "yml"]:
        return yaml.safe_load(open(path, "r"))
    elif ext in ["json"]:
        return json.load(open(path, "r"))


def dump(obj: Any, path: Path) -> None:
    """Dumps an object to a file with a supported markup format.
    Supported formats include:
    - yaml
    - json
    Args:
        obj (Any): The object to dump.
        path (Path): Path to the file to write.
    """
    ext = get_extension(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if ext in ["yaml", "yml"]:
        yaml.safe_dump(obj, open(path, "w"))
    elif ext in ["json"]:
        json.dump(obj, open(path, "w"))
