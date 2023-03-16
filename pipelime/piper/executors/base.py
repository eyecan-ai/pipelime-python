from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Union, Sequence

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

    def _log_message(self, message: str, *, token: str):
        logger.debug(f"[prefix={self._node_prefix},token={token}] {message}")

    def preproc_node(
        self,
        node: GraphNodeOperation,
        graph: DAGNodesGraph,
        token: str,
        force_gc: Union[bool, str, Sequence[str]],
    ):
        self._set_piper_info(node=node, token=token)
        self._log_message(f"Executing command: {node.command._piper.node}", token=token)

    def postproc_node(
        self,
        node: GraphNodeOperation,
        graph: DAGNodesGraph,
        token: str,
        force_gc: Union[bool, str, Sequence[str]],
    ):
        if isinstance(force_gc, str):
            force_gc = [force_gc]

        if (isinstance(force_gc, bool) and force_gc) or (
            isinstance(force_gc, Sequence) and node.command._piper.node in force_gc
        ):
            import gc

            self._log_message("Forcing garbage collection", token=token)
            gc.collect()

        if self.task:
            self.task.advance()
        self._log_message("Command execution completed", token=token)

    def __call__(
        self,
        graph: DAGNodesGraph,
        token: str = "",
        force_gc: Union[bool, str, Sequence[str]] = False,
    ) -> bool:
        self._log_message("Graph execution started", token=token)
        if self.task:
            self.task.restart()

        ret = self.exec(graph=graph, token=token, force_gc=force_gc)

        if self.task:
            self.task.finish()
        self._log_message("Graph execution completed", token=token)
        return ret

    @abstractmethod
    def exec(
        self,
        graph: DAGNodesGraph,
        token: str,
        force_gc: Union[bool, str, Sequence[str]],
    ) -> bool:
        """Executes the given NodesGraph. Subclasses should call ``self.preproc_node``
        and ``self.postproc_node`` before and after the execution of each node.

        Args:
            graph (DAGNodesGraph): input DAGNodesGraph
            token (str, optional): execution token shared among nodes. Defaults to "".
            force_gc (Union[bool, str, Sequence[str]], optional): force garbage
                collection after each node execution, if True, or after the execution of
                the given node(s). Defaults to False.

        Returns:
            bool: TRUE if the execution was successful, FALSE otherwise
        """


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
        self, executor: NodesGraphExecutor, listener_clbk: Union[str, Path, None] = None
    ) -> None:
        self._executor = executor
        self._listener_clbk = listener_clbk

    def __call__(
        self,
        graph: DAGNodesGraph,
        token: str = "",
        force_gc: Union[bool, str, Sequence[str]] = False,
    ) -> bool:
        if isinstance(self._listener_clbk, Path):
            callback = ListenerCallbackFactory.get_callback(
                "FILE", filename=self._listener_clbk
            )
        else:
            callback = ListenerCallbackFactory.get_callback(
                self._listener_clbk or ListenerCallbackFactory.DEFAULT_CALLBACK_TYPE
            )

        logger.disable(self._executor.__module__)
        logger.disable(self.__module__)
        receiver = ProgressReceiverFactory.get_receiver(token)
        listener = Listener(receiver, callback)
        try:
            listener.start()
            res = self._executor(graph, token, force_gc)
        finally:
            listener.stop()
            logger.enable(self._executor.__module__)
        return res

    def exec(
        self,
        graph: DAGNodesGraph,
        token: str,
        force_gc: Union[bool, str, Sequence[str]],
    ) -> bool:
        raise NotImplementedError()
