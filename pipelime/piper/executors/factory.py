from typing import Optional, Union

from pipelime.piper.executors.base import NodesGraphExecutor, WatcherNodesGraphExecutor
from pipelime.piper.executors.naive import NaiveNodesGraphExecutor
from pipelime.piper.progress.tracker.base import TrackedTask


class NodesGraphExecutorFactory:
    CLASS_MAP = {
        "NAIVE": NaiveNodesGraphExecutor,
    }

    DEFAULT_EXECUTOR_TYPE = "NAIVE"

    @classmethod
    def get_executor(
        cls,
        type_: Optional[str] = None,
        node_prefix: str = "",
        watch: Union[bool, str] = False,
        task: Optional[TrackedTask] = None,
        **kwargs,
    ) -> NodesGraphExecutor:
        executor = cls.CLASS_MAP[(type_ or cls.DEFAULT_EXECUTOR_TYPE).upper()](
            node_prefix=node_prefix, **kwargs
        )
        executor.task = task

        if watch:
            executor = WatcherNodesGraphExecutor(
                executor, watch if isinstance(watch, str) else None
            )

        return executor
