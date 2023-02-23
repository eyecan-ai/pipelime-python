import itertools
from abc import ABC, abstractmethod
from pathlib import Path
from pydantic import BaseModel
from typing import (
    Optional,
    Union,
    Sequence,
    Mapping,
    Set,
    Iterable,
    Iterator,
    Collection,
    Any,
)

import networkx as nx

from pipelime.piper.model import DAGModel, PipelimeCommand


class GraphNode(ABC):
    GRAPH_NODE_TYPE_OPERATION = "operation"
    GRAPH_NODE_TYPE_DATA = "data"

    def __init__(self, name: str, type: str):
        self.name = str(name)
        self.type = str(type)

    @property
    def id(self) -> str:
        return f"{self.type}({self.name})"

    @property
    @abstractmethod
    def readable_repr(self) -> str:
        pass

    def __hash__(self) -> int:
        return hash(self.id)

    def __repr__(self) -> str:
        from hashlib import blake2b as hashfn

        h = hashfn()
        h.update(self.id.encode())
        return h.hexdigest()

    def __eq__(self, o) -> bool:
        return self.id == o.id


class GraphNodeOperation(GraphNode):
    def __init__(self, name: str, command: PipelimeCommand, show_command_name: bool):
        super().__init__(name, GraphNode.GRAPH_NODE_TYPE_OPERATION)
        self._command = command
        self._show_command_name = show_command_name

    @property
    def command(self) -> PipelimeCommand:
        return self._command

    @property
    def readable_repr(self) -> str:
        return (
            f"{self.name}: {self._command.command_name}"
            if self._show_command_name
            else self.name
        )


class GraphNodeData(GraphNode):
    def __init__(
        self,
        name: str,
        path: str,
        data_max_width: Union[int, str, None],
        ellipsis_position: str,
    ):
        import math

        super().__init__(name, GraphNode.GRAPH_NODE_TYPE_DATA)
        self._path = str(path)

        self._readable_repr = self._path
        if isinstance(data_max_width, int):
            if data_max_width > 0 and len(self._readable_repr) > data_max_width:
                ellipsis_position = ellipsis_position.lower()
                effective_length = data_max_width - 3
                if ellipsis_position == "start":
                    self._readable_repr = f"…{self._readable_repr[-effective_length:]}"
                elif ellipsis_position == "end":
                    self._readable_repr = f"{self._readable_repr[:effective_length]}…"
                elif ellipsis_position == "middle":
                    halfw = effective_length * 0.5
                    self._readable_repr = (
                        self._readable_repr[: math.floor(halfw)]
                        + "..."
                        + self._readable_repr[-math.ceil(halfw) :]
                    )
                else:
                    raise ValueError(f"Unknown ellipsis position: {ellipsis_position}")
        elif data_max_width is not None:
            if ellipsis_position == "start":
                idx = self._readable_repr.rfind(data_max_width)
                if idx >= 0:
                    self._readable_repr = (
                        f"…{self._readable_repr[idx+len(data_max_width):]}"
                    )
            elif ellipsis_position == "end":
                idx = self._readable_repr.find(data_max_width)
                if idx >= 0:
                    self._readable_repr = f"{self._readable_repr[:idx]}…"
            elif ellipsis_position == "middle":
                self._readable_repr = self._readable_repr.replace(data_max_width, "…")
            elif ellipsis_position == "regex":
                import re

                self._readable_repr = re.sub(data_max_width, "…", self._readable_repr)

    @property
    def path(self) -> str:
        return self._path

    @property
    def readable_repr(self) -> str:
        return self._readable_repr


class DAGNodesGraph:
    class GraphAttrs:
        INPUT_PORT = "input_port"
        OUTPUT_PORT = "output_port"
        EDGE_TYPE = "edge_type"

    def __init__(self, raw_graph: nx.DiGraph):
        """Initialize the DAG graph starting from a raw directed graph (networkx).

        Args:
            raw_graph (nx.DiGraph): The raw graph to be converted to a DAG graph.
        """
        super().__init__()
        self._raw_graph = raw_graph

    @property
    def raw_graph(self) -> nx.DiGraph:
        """The raw graph (networkx) of the DAG.

        Returns:
            nx.DiGraph: The raw graph (networkx) of the DAG.
        """
        return self._raw_graph

    @property
    def operations_graph(self) -> "DAGNodesGraph":
        """The operations graph (networkx) of the DAG. It is a filtered version of the
        raw graph with operations nodes only.

        Returns:
            DAGNodesGraph: The operations graph (networkx) of the DAG.
        """
        return DAGNodesGraph.filter_node_graph(
            self, [GraphNode.GRAPH_NODE_TYPE_OPERATION]
        )

    @property
    def data_graph(self) -> "DAGNodesGraph":
        """The data graph (networkx) of the DAG. It is a filtered version of the
        raw graph with data nodes only.

        Returns:
            DAGNodesGraph: The data graph (networkx) of the DAG.
        """
        return DAGNodesGraph.filter_node_graph(self, [GraphNode.GRAPH_NODE_TYPE_DATA])

    @property
    def root_nodes(self) -> Iterator[GraphNode]:
        """The root nodes of the DAG. They are the nodes that have no predecessors.

        Returns:
            Iterator[GraphNode]: The root nodes of the DAG.
        """
        for node in self.raw_graph.nodes:
            if not any(True for _ in self.raw_graph.predecessors(node)):
                yield node

    @property
    def leaf_nodes(self) -> Iterator[GraphNode]:
        """The leaf nodes of the DAG. They are the nodes that have no successors.

        Returns:
            Iterator[GraphNode]: The leaf nodes of the DAG.
        """
        for node in self.raw_graph.nodes:
            if not any(True for _ in self.raw_graph.successors(node)):
                yield node

    @property
    def input_data_nodes(self) -> Iterator[GraphNodeData]:
        """The root data nodes of the DAG. They are the data nodes that have no
        predecessors.

        Returns:
            Iterator[GraphNodeData]: The input data nodes of the DAG.
        """
        for x in self.root_nodes:
            if isinstance(x, GraphNodeData):
                yield x

    @property
    def output_data_nodes(self) -> Iterator[GraphNodeData]:
        """The leaf data nodes of the DAG. They are the data nodes that have no
        successors.

        Returns:
            Iterator[GraphNodeData]: The output data nodes of the DAG.
        """
        for x in self.leaf_nodes:
            if isinstance(x, GraphNodeData):
                yield x

    def get_input_port_name(self, data_node: GraphNodeData) -> str:
        """Returns the name of the input port a data node is connected to."""
        ports = nx.get_edge_attributes(self.raw_graph, self.GraphAttrs.INPUT_PORT)
        for edge, name in ports.items():
            if edge[0] is data_node:
                return name
        return ""

    def get_output_port_name(self, data_node: GraphNodeData) -> str:
        """Returns the name of the output port a data node is connected to."""
        ports = nx.get_edge_attributes(self.raw_graph, self.GraphAttrs.OUTPUT_PORT)
        for edge, name in ports.items():
            if edge[1] is data_node:  # type: ignore
                return name
        return ""

    def write_graphml(self, path_or_stream):
        """Write the graph to a GraphML file.

        Args:
            path_or_stream (Union[str, Path]): The path to the GraphML file.
        """
        nx.write_graphml(
            self.raw_graph,
            str(path_or_stream) if isinstance(path_or_stream, Path) else path_or_stream,
        )

    def consumable_operations(
        self,
        produced_data: Set[GraphNodeData],
    ) -> Set[GraphNodeOperation]:
        """Given a set of produced data, returns the set of operations that can be
        consumed given the produced data, i.e. the operations that have inputs available

        Args:
            produced_data (Set[GraphNodeData]): The set of produced data.

        Returns:
            Set[GraphNodeOperation]: The set of operations that can be consumed given
            the produced data.
        """

        consumables = set()
        for node in self.operations_graph.raw_graph.nodes:
            in_data = [
                x
                for x in self.raw_graph.predecessors(node)
                if isinstance(x, GraphNodeData)
            ]
            if all(x in produced_data for x in in_data):
                consumables.add(node)
        return consumables

    def consume(
        self,
        operation_nodes: Collection[GraphNodeOperation],
    ) -> Set[GraphNodeData]:
        """Given a set of operations, consume them and return the set of produced data,
        i.e. the data are the outputs of the operations.

        Args:
            operation_nodes (Collection[GraphNodeOperation]): The set of operations
                to be consumed.

        Returns:
            Set[GraphNodeData]: The set of produced data.
        """
        consumed_data = set()
        for node in operation_nodes:
            out_data = [
                x
                for x in self.raw_graph.successors(node)
                if isinstance(x, GraphNodeData)
            ]
            consumed_data.update(out_data)
        return consumed_data

    def build_execution_stack(self) -> Sequence[Set[GraphNodeOperation]]:
        """Builds the execution stack of the DAG. The execution stack is a list of lists
        of operations. Each list of operations is a virtual layer of operations that
        can be executed in parallel because they have no dependencies.

        Returns:
            Sequence[Sequence[GraphNodeOperation]]: The execution stack of the DAG.
        """

        # the execution stack is a list of lists of nodes. Each list represents a
        execution_stack: Sequence[Set[GraphNodeOperation]] = []

        # initalize global produced data with the root nodes
        global_produced_data = set()
        global_produced_data.update(
            [x for x in self.root_nodes if isinstance(x, GraphNodeData)]
        )

        # set of operations that have been consumed
        consumed_operations = set()

        while True:
            # which operations can be consumed given the produced data?
            consumable: set = self.consumable_operations(global_produced_data)

            # Remove from consumable operations the ones that have already been consumed
            consumable = consumable.difference(consumed_operations)

            # No consumable operations? We are done!
            if len(consumable) == 0:
                break

            # If not empty, append consumable operations to the execution stack
            execution_stack.append(consumable)

            # The set of produced data is the union of all the consumed operations
            produced_data: set = self.consume(consumable)

            # Add the consumed operations to the consumed operations set
            consumed_operations.update(consumable)

            # Add the produced data to the global produced data
            global_produced_data.update(produced_data)

        return execution_stack

    @classmethod
    def build_nodes_graph(
        cls,
        dag_model: DAGModel,
        data_max_width: Union[int, str, None] = None,
        show_command_name: bool = False,
        ellipsis_position: str = "middle",
    ) -> "DAGNodesGraph":
        """Builds the nodes graph of the DAG starting from a plain DAG model.

        Args:
            dag_model (DAGModel): the DAG model.
            data_max_width (Union[int, str, None], optional): If an int is given, it is
                the maximum data node name length. If a string is given, it is matched
                against the node name and replaced with ellipses. Defaults to None.
            show_command_name (bool, optional): whether to show the command name
                alongside the node name. Defaults to False.
            ellipsis_position (str, optional): where to put the ellipses if the data
                node name is too long, can be "start", "middle", "end" or "regex".
                Defaults to "middle".

        Returns:
            DAGNodesGraph: The nodes graph of the DAG.
        """
        g = nx.DiGraph()

        for node_name, node in dag_model.nodes.value.items():
            node: PipelimeCommand

            inputs = node.get_inputs()
            outputs = node.get_outputs()

            cls._add_io_ports(
                g,
                node_name,
                node,
                inputs,
                True,
                data_max_width,
                ellipsis_position,
                show_command_name,
            )
            cls._add_io_ports(
                g,
                node_name,
                node,
                outputs,
                False,
                data_max_width,
                ellipsis_position,
                show_command_name,
            )

        return DAGNodesGraph(raw_graph=g)

    @classmethod
    def _add_io_ports(
        cls,
        di_graph: nx.DiGraph,
        node_name: str,
        node_cmd: PipelimeCommand,
        io_map: Optional[Mapping[str, Any]],
        is_input: bool,
        data_max_width: Union[int, str, None],
        ellipsis_position: str,
        show_command_name: bool,
    ):
        def _to_str(x):
            if hasattr(x, "__piper_repr__"):
                return x.__piper_repr__()
            if isinstance(x, (str, bytes)):
                return str(x)
            if isinstance(x, Path):
                return x.resolve().absolute().as_posix()
            return repr(x)

        if io_map is not None:
            for name, value in io_map.items():
                if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
                    value = [value]

                add_index = len(value) > 1
                attrs = {}
                for idx, x in enumerate(value):
                    if x:  # discard empty strings and None
                        x_name = _to_str(x)
                        n0 = GraphNodeData(
                            x_name, x_name, data_max_width, ellipsis_position
                        )
                        n1 = GraphNodeOperation(node_name, node_cmd, show_command_name)
                        if is_input:
                            port_attr = DAGNodesGraph.GraphAttrs.INPUT_PORT
                            edge_attr = "DATA_2_OPERATION"
                        else:
                            port_attr = DAGNodesGraph.GraphAttrs.OUTPUT_PORT
                            edge_attr = "OPERATION_2_DATA"
                            n0, n1 = n1, n0

                        di_graph.add_edge(n0, n1)
                        attrs.update(
                            {
                                (n0, n1): {
                                    port_attr: f"{name}[{idx}]" if add_index else name,
                                    DAGNodesGraph.GraphAttrs.EDGE_TYPE: edge_attr,
                                }
                            }
                        )
                nx.set_edge_attributes(di_graph, attrs)

    @classmethod
    def filter_raw_graph(
        cls,
        full_graph: nx.DiGraph,
        types: Sequence[str],
    ) -> nx.DiGraph:
        """Filters the raw graph of the DAG to keep only the nodes of the given types.

        Args:
            full_graph (nx.DiGraph): The raw graph of the DAG.
            types (Sequence[str]): The types of the nodes to keep.

        Returns:
            nx.DiGraph: The filtered raw graph of the DAG.
        """

        filtered_graph: nx.DiGraph = nx.DiGraph()

        for node in full_graph.nodes:
            if node.type not in types:
                predecessors = list(full_graph.predecessors(node))
                successors = list(full_graph.successors(node))
                pairs = list(itertools.product(predecessors, successors))
                for pair in pairs:
                    filtered_graph.add_edge(pair[0], pair[1])

        # There could be single layers graph without connections,
        # all nodes have to be kept and are therefore added to the graph
        for node in full_graph.nodes:
            if node.type in types:
                filtered_graph.add_node(node)

        return filtered_graph

    @classmethod
    def filter_node_graph(
        cls,
        nodes_graph: "DAGNodesGraph",
        types: Sequence[str],
    ) -> "DAGNodesGraph":
        """Filters the nodes graph of the DAG to keep only the nodes of the given types.
        It wraps the raw graph into a DAGNodesGraph.

        Args:
            nodes_graph (DAGNodesGraph): The nodes graph of the DAG.
            types (Sequence[str]): The types of the nodes to keep.

        Returns:
            DAGNodesGraph: The filtered nodes graph of the DAG.
        """
        return DAGNodesGraph(
            raw_graph=cls.filter_raw_graph(nodes_graph.raw_graph, types)
        )
