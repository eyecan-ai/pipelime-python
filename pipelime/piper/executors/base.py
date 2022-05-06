from abc import ABC, abstractmethod
from typing import Any, Sequence
from pipelime.pipes.graph import DAGNodesGraph
from pipelime.pipes.model import NodeModel
from loguru import logger
from pipelime.pipes.watcher import Watcher


class NodeModelExecutionParser(ABC):
    """A NodeModelExecutionParser is an abstract class that should implement the parsing
    of a NodeModel into a bash command and arguments.
    """

    def __init__(self) -> None:
        pass

    @abstractmethod
    def build_command_chunks(self, node_model: NodeModel) -> Sequence[Any]:
        """Builds the command chunks for the given node model. Chunks are pieces of the command
        that usually are separated by spaces.

        Args:
            node_model (NodeModel): input node model

        Returns:
            Sequence[Any]: command chunks
        """
        raise NotImplementedError()

    def build_plain_command(self, node_model: NodeModel) -> str:
        """Builds the plain command for the given node model. It joins the command chunks with
        spaces.

        Args:
            node_model (NodeModel): input node model

        Returns:
            str: plain bash command
        """
        return " ".join(self.build_command_chunks(node_model=node_model))


class NodesGraphExecutor(ABC):
    """An NodesGraphExecutor is an abstract class that should implement the execution of a
    NodesGraph made of Nodes.

    """

    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def exec(self, graph: DAGNodesGraph) -> bool:
        """Executes the given NodesGraph.

        Args:
            graph (DAGNodesGraph): input DAGNodesGraph

        Returns:
            bool: TRUE if the execution was successful, FALSE otherwise
        """
        raise NotImplementedError()


class WatcherNodesGraphExecutor(NodesGraphExecutor):
    """Decorator for `NodesGraphExecutor` that wraps an existing executor.

    The `WatcherNodesGraphExecutor` disables all logs from the wrapped executor and
    lets a `Watcher` print live updates in the form of a rich table.

    This wrapper can be used with any subtype of `NodesGraphExecutor`::

        # Different types of executor
        executor1 = NaiveNodesGraphExecutor(token)
        executor2 = AdvancedNodesGraphExecutor(token)
        executor3 = SuperCoolNodesGraphExecutor(token)

        # All of them can be transformed into a `WatcherNodesGraphExecutor`
        # regardless of their type.
        executor1 = WatcherNodesGraphExecutor(executor1)
        executor2 = WatcherNodesGraphExecutor(executor2)
        executor3 = WatcherNodesGraphExecutor(executor3)
    """

    def __init__(self, executor: NodesGraphExecutor) -> None:
        self._executor = executor

    def exec(self, graph: DAGNodesGraph, token: str = "") -> bool:
        res = False
        logger.disable(self._executor.__module__)
        watcher = Watcher(token)
        try:
            watcher.watch()
            res = self._executor.exec(graph, token)
        finally:
            watcher.stop()
            logger.enable(self._executor.__module__)
        return res
