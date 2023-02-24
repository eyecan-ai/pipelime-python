from abc import ABC, abstractmethod
from typing import Optional

from loguru import logger

from pipelime.piper.graph import DAGNodesGraph, GraphNodeOperation
from pipelime.piper.progress.listener.base import Listener
from pipelime.piper.progress.listener.factory import (
    ListenerCallbackFactory,
    ProgressReceiverFactory,
)
from pipelime.piper.progress.tracker.base import TrackedTask


class NodesGraphExecutor(ABC):
    """An NodesGraphExecutor is an abstract class that should
    implement the execution of a NodesGraph made of Nodes.
    """

    def __init__(self, node_prefix: str) -> None:
        super().__init__()
        self._node_prefix = node_prefix
        self._task = None

    @property
    def task(self) -> Optional[TrackedTask]:
        return self._task

    @task.setter
    def task(self, task: Optional[TrackedTask]):
        self._task = task

    def _set_piper_info(self, node: GraphNodeOperation, token: str):
        node.command.set_piper_info(token=token, node=self._node_prefix + node.name)

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
    lets a `Watcher` print live updates.

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

    def __init__(
        self, executor: NodesGraphExecutor, listener_clbk: Optional[str] = None
    ) -> None:
        self._executor = executor
        self._listener_clbk = listener_clbk

    def exec(self, graph: DAGNodesGraph, token: str = "") -> bool:
        res = False
        logger.disable(self._executor.__module__)
        receiver = ProgressReceiverFactory.get_receiver(token)
        callback = ListenerCallbackFactory.get_callback(
            self._listener_clbk or ListenerCallbackFactory.DEFAULT_CALLBACK_TYPE
        )
        listener = Listener(receiver, callback)
        try:
            listener.start()
            res = self._executor.exec(graph, token)
        finally:
            listener.stop()
            logger.enable(self._executor.__module__)
        return res
