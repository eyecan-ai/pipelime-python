import itertools
from pydantic import BaseModel
from typing import Optional, Sequence, Mapping, Set, Iterable, Collection, Any

import networkx as nx

from pipelime.piper.model import DAGModel, PipelimeCommand


class GraphNode:
    GRAPH_NODE_TYPE_OPERATION = "operation"
    GRAPH_NODE_TYPE_DATA = "data"

    def __init__(self, name: str, type: str):
        self.name = str(name)
        self.type = str(type)

    @property
    def id(self):
        return f"{self.type}({self.name})"

    def __hash__(self) -> int:
        return hash(f"{self.type}({self.name})")

    def __repr__(self) -> str:
        return f"{self.type}({self.name})"

    def __eq__(self, o) -> bool:
        return self.id == o.id


class GraphNodeOperation(GraphNode):
    def __init__(self, name: str, command: PipelimeCommand):
        super().__init__(name, GraphNode.GRAPH_NODE_TYPE_OPERATION)
        self._command = command

    @property
    def command(self) -> PipelimeCommand:
        return self._command


class GraphNodeData(GraphNode):
    def __init__(self, name: str, path: str):
        super().__init__(name, GraphNode.GRAPH_NODE_TYPE_DATA)
        self._path = str(path)

    @property
    def path(self) -> str:
        return self._path


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
    def root_nodes(self) -> Sequence[GraphNode]:
        """The root nodes of the DAG. They are the nodes that have no predecessors.

        Returns:
            Sequence[GraphNode]: The root nodes of the DAG.
        """
        return [
            node
            for node in self.raw_graph.nodes
            if len(list(self.raw_graph.predecessors(node))) == 0
        ]

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
    ) -> "DAGNodesGraph":
        """Builds the nodes graph of the DAG starting from a plain DAG model.

        Returns:
            DAGNodesGraph: The nodes graph of the DAG.
        """

        g = nx.DiGraph()

        for node_name, node in dag_model.nodes.items():
            node: PipelimeCommand

            inputs = node.get_inputs()
            outputs = node.get_outputs()

            cls._add_io_ports(g, node_name, node, inputs, True)
            cls._add_io_ports(g, node_name, node, outputs, False)

        return DAGNodesGraph(raw_graph=g)

    @classmethod
    def _add_io_ports(
        cls,
        di_graph: nx.DiGraph,
        node_name: str,
        node_cmd: PipelimeCommand,
        io_map: Optional[Mapping[str, Any]],
        is_input: bool,
    ):
        def _to_str(x):
            if hasattr(x, "__piper_repr__"):
                return x.__piper_repr__()
            return repr(x)

        if io_map is not None:
            for name, value in io_map.items():
                if (
                    isinstance(value, str)
                    or isinstance(value, BaseModel)
                    or not isinstance(value, Iterable)
                ):
                    value = [value]

                attrs = {}
                for x in value:
                    if x:  # discard empty strings and None
                        x_name = _to_str(x)
                        n0 = GraphNodeData(x_name, x_name)
                        n1 = GraphNodeOperation(node_name, node_cmd)
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
                                    port_attr: name,
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
