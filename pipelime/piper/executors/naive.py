from loguru import logger

from pipelime.piper.executors.base import NodesGraphExecutor
from pipelime.piper.graph import DAGNodesGraph


class NaiveNodesGraphExecutor(NodesGraphExecutor):
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

        if self.task:
            self.task.restart()

        for layer in graph.build_execution_stack():
            for node in layer:
                self._set_piper_info(node=node, token=token)

                logger.debug(
                    f"[token={token}] Executing command: {node.command._piper.node}"
                )

                try:
                    node.command()
                except Exception as e:
                    raise RuntimeError(
                        f"[token={token}] Failed to execute node: {node.name}"
                    ) from e

                if self.task:
                    self.task.advance()

        logger.debug(f"[token={token}] Graph execution completed")
        if self.task:
            self.task.finish()

        return True
