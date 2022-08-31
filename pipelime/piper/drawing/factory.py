from typing import Sequence

from pipelime.piper.drawing.base import NodesGraphDrawer
from pipelime.piper.drawing.graphviz import GraphvizNodesGraphDrawer
from pipelime.piper.drawing.mermaid import MermaidNodesGraphDrawer


class NodesGraphDrawerFactory:

    FACTORY_MAP = {
        "graphviz": GraphvizNodesGraphDrawer,
        "mermaid": MermaidNodesGraphDrawer,
    }

    @classmethod
    def create(cls, backend: str) -> NodesGraphDrawer:
        if backend not in cls.FACTORY_MAP:
            raise ValueError(f"Backend {backend} not supported")
        return cls.FACTORY_MAP[backend]()

    @classmethod
    def available_backends(cls) -> Sequence[str]:
        return list(cls.FACTORY_MAP.keys())
