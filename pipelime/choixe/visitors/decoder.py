import json
from pathlib import Path
from typing import Any

import numpy as np
from pydantic import BaseModel

from pipelime.choixe.ast.nodes import LiteralNode, Node
from pipelime.choixe.visitors.unparser import Unparser


class Decoder(Unparser):
    """Specialization of the `Unparser` for the decode operation."""

    def visit_object(self, node: LiteralNode) -> Any:
        data = super().visit_object(node)
        if isinstance(data, np.ndarray):
            return data.tolist()
        elif "numpy" in str(type(data)):
            return data.item()
        elif isinstance(data, BaseModel):
            symbol = f"{data.__module__}.{data.__class__.__qualname__}"
            return {"$model": symbol, "$args": json.loads(data.json(by_alias=True))}
        elif isinstance(data, Path):
            return str(data)
        else:
            return data


def decode(node: Node) -> Any:
    """A special unparsing operation that also converts some object types like numpy
    arrays into built-in lists that are supported by common markup formats like yaml and
    json.

    Args:
        node (Node): The Choixe AST node to decode.

    Returns:
        List[Tuple[List[Union[str, int]], Any]]: The decoded unparsed node.
    """
    return node.accept(Decoder())
