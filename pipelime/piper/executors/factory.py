from pipelime.piper.executors.base import NodesGraphExecutor, WatcherNodesGraphExecutor
from pipelime.piper.executors.naive import NaiveNodesGraphExecutor


class NodesGraphExecutorFactory:
    CLASS_MAP = {
        "NAIVE": NaiveNodesGraphExecutor,
    }

    DEFAULT_EXECUTOR_TYPE = "NAIVE"

    @classmethod
    def get_executor(
        cls, type_: str = DEFAULT_EXECUTOR_TYPE, watch: bool = False, **kwargs
    ) -> NodesGraphExecutor:
        executor = cls.CLASS_MAP[type_](**kwargs)

        if watch:
            executor = WatcherNodesGraphExecutor(executor)

        return executor
