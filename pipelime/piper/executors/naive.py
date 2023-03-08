from typing import Union, Sequence

from pipelime.piper.executors.base import NodesGraphExecutor
from pipelime.piper.graph import DAGNodesGraph


class NaiveNodesGraphExecutor(NodesGraphExecutor):
    def exec(
        self,
        graph: DAGNodesGraph,
        token: str,
        force_gc: Union[bool, str, Sequence[str]],
    ) -> bool:
        """Executes the given graph.

        Args:
            graph (DAGNodesGraph): target graph
            token (str, optional): execution token shared among nodes. Defaults to "".
            force_gc (Union[bool, str, Sequence[str]], optional): force garbage
                collection after each node execution, if True, or after the execution of
                the given node(s). Defaults to False.

        Raises:
            RuntimeError: if some node fails to execute

        Returns:
            bool: True if the execution succeeds
        """
        for layer in graph.build_execution_stack():
            for node in layer:
                self.preproc_node(node, graph, token, force_gc)

                try:
                    node.command()
                except Exception as e:
                    raise RuntimeError(f"Failed to execute node: {node.name}") from e

                self.postproc_node(node, graph, token, force_gc)

        return True
