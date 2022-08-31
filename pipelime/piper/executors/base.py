from abc import ABC, abstractmethod

from loguru import logger

from pipelime.piper.graph import DAGNodesGraph
from pipelime.piper.progress.listener.base import Listener
from pipelime.piper.progress.listener.factory import (
    ListenerCallbackFactory,
    ProgressReceiverFactory,
)


class NodesGraphExecutor(ABC):
    """An NodesGraphExecutor is an abstract class that should
    implement the execution of a NodesGraph made of Nodes.
    """

    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def exec(self, graph: DAGNodesGraph, token: str = "") -> bool:
        """Executes the given NodesGraph.

        Args:
            graph (DAGNodesGraph): input DAGNodesGraph
            token (str, optional): execution token shared among nodes. Defaults to "".

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
        receiver = ProgressReceiverFactory.get_receiver(token)
        callback = ListenerCallbackFactory.get_callback()
        listener = Listener(receiver, callback)
        try:
            listener.start()
            res = self._executor.exec(graph, token)
        finally:
            listener.stop()
            logger.enable(self._executor.__module__)
        return res
