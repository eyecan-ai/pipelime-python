from loguru import logger

from pipelime.piper.executors.base import NodesGraphExecutor
from pipelime.piper.graph import DAGNodesGraph, GraphNodeOperation


class NaiveGraphExecutor(NodesGraphExecutor):
    def exec(self, graph: DAGNodesGraph, token: str = "") -> bool:
        """Executes the given graph.

        Args:
            graph (DAGNodesGraph): target graph
            token (str, optional): execution token shared among nodes. Defaults to "".

        Raises:
            RuntimeError: if some node fails to execute

        Returns:
            bool: True if the execution succeeds
        """

        for layer in graph.build_execution_stack():
            for node in layer:
                node.command._piper.token = token
                node.command._piper.node = node.name

                logger.debug(f"Executing command: {node.command._piper.node}")

                node.command()
