from typing import Optional

from pipelime.piper.executors.base import NodesGraphExecutor, WatcherNodesGraphExecutor
from pipelime.piper.executors.naive import NaiveNodesGraphExecutor


class NodesGraphExecutorFactory:
    CLASS_MAP = {
        "NAIVE": NaiveNodesGraphExecutor,
    }

    DEFAULT_EXECUTOR_TYPE = "NAIVE"

    @classmethod
    def get_executor(
        cls, type_: Optional[str] = None, watch: bool = False, **kwargs
    ) -> NodesGraphExecutor:
        executor = cls.CLASS_MAP[(type_ or cls.DEFAULT_EXECUTOR_TYPE).upper()](**kwargs)

        if watch:
            executor = WatcherNodesGraphExecutor(executor)

        return executor
