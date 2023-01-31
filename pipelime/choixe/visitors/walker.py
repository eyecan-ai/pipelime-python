from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Sequence, Tuple, Union

from pipelime.choixe.ast.nodes import Node
from pipelime.choixe.visitors.unparser import Unparser


@dataclass
class Entry:
    key: Sequence[Union[str, int]]
    value: Any

    def prepend(self, key) -> Entry:
        return Entry([key, *self.key], self.value)


@dataclass
class Chunk:
    entries: Sequence[Entry]

    def prepend(self, key: Union[int, str]) -> Chunk:
        return Chunk([x.prepend(key) for x in self.entries])

    def __add__(self, other: Chunk) -> Chunk:
        return Chunk(list(self.entries) + list(other.entries))


# 🏜️🤠🌵
class Walker(Unparser):
    """Specialization of the `Unparser` for the walk operation."""

    def __init__(self) -> None:
        super().__init__()

        # Wrap all visiting methods adding a chunkify step.
        for x in dir(self):
            if x.startswith("visit_"):
                setattr(self, x, self._wrapped_visit(getattr(self, x)))

    def _chunkify(self, data: Dict) -> Chunk:
        chunk = Chunk([])

        for k, v in data.items():
            if not isinstance(v, Chunk):
                v = Chunk([Entry([], v)])

            chunk += v.prepend(k)

        return chunk

    def _wrapped_visit(self, func: Callable[[Node], Any]) -> Callable[[Node], Any]:
        def wrapper(node: Node):
            data = func(node)

            if isinstance(data, List):
                data = dict(enumerate(data))

            # NB: empty dicts must not be chunkified!
            if isinstance(data, Dict) and data:
                return self._chunkify(data)
            else:
                return data

        return wrapper


def walk(node: Node) -> List[Tuple[List[Union[str, int]], Any]]:
    """A special unparsing operation that also flattens a structure into a list of
    (deep_key, value) tuples. A deep key is a list of either strings or integers, that
    can be used as a pydash path.

    Example::

        data = {
            "a": 10,
            "b": [
                "bob",
                {
                    "a": 105,
                    "b": "alice",
                }
            ]
        }
        walk(parse(data))
        # [
        #     (["a"], 10),
        #     (["b", 0], "bob"),
        #     (["b", 1, "a"], 105),
        #     (["b", 1, "b"], "alice"),
        # ]

    Args:
        node (Node): The Choixe AST node to walk.

    Returns:
        List[Tuple[List[Union[str, int]], Any]]: The flattened unparsed node.
    """
    chunk: Chunk = node.accept(Walker())
    if not isinstance(chunk, Chunk):
        return [([], chunk)]
    return [(list(x.key), x.value) for x in chunk.entries]
