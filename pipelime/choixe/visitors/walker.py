from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Sequence, Tuple, Union

from pipelime.choixe.ast.nodes import DictNode, ListNode, Node
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

    def visit_dict(self, node: DictNode) -> Chunk:
        chunk = Chunk([])
        for k, v in node.nodes.items():
            key = k.accept(self)
            value = v.accept(self)
            assert isinstance(key, str), "Only string keys are allowed in dict walk"

            if not isinstance(value, Chunk):
                value = Chunk([Entry([], value)])

            chunk += value.prepend(key)

        return chunk

    def visit_list(self, node: ListNode) -> Chunk:
        chunk = Chunk([])
        for i, x in enumerate(node.nodes):
            value = x.accept(self)

            if not isinstance(value, Chunk):
                value = Chunk([Entry([], value)])

            chunk += value.prepend(i)

        return chunk


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
