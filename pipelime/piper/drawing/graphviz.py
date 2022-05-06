from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Optional, Sequence

import numpy as np
import pygraphviz as pgv

from pipelime.piper.drawing.base import NodesGraphDrawer
from pipelime.piper.graph import (
    DAGNodesGraph,
    GraphNode,
    GraphNodeData,
    GraphNodeOperation,
)


class GraphvizNodesGraphDrawer(NodesGraphDrawer):
    def _is_node_file(self, node: GraphNodeData) -> bool:
        """Returns True if the node path is (or could be) a file

        Args:
            node (GraphNodeData):  input node

        Returns:
            bool:  True if the node path is (or could be) a file
        """
        return len(Path(node.path).suffix) > 0

    def _operation_shape(self, node: GraphNodeOperation) -> str:
        """Returns the shape string of an operation node

        Args:
            node (GraphNodeOperation): input node

        Returns:
            str: shape of the node
        """
        return "box3d"

    def _data_shape(self, node: GraphNodeData) -> str:
        """Returns the shape string of a data node

        Args:
            node (GraphNodeData): input node

        Returns:
            str: shape of the node
        """
        return "note" if self._is_node_file(node) else "cylinder"

    def _operation_styles(self, node: GraphNodeOperation) -> dict:
        """Returns the style attributes for a generic Operation node

        Args:
            node (GraphNodeOperation): input node

        Returns:
            dict: style attributes for the node
        """
        return {
            "style": "filled",
            "fillcolor": "#03A9F4",
            "fontcolor": "#fafafa",
        }

    def _data_styles(self, node: GraphNodeData) -> dict:
        """Returns the style attributes for a generic Data node

        Args:
            node (GraphNodeData): input node

        Returns:
            dict: style attributes for the node
        """
        if self._is_node_file(node):
            return {
                "style": "filled",
                "fillcolor": "#fafafa",
                "fontcolor": "#222222",
            }
        else:
            return {
                "style": "filled",
                "fillcolor": "#009688",
                "fontcolor": "#fafafa",
            }

    def _operation_attributes(self, node: GraphNodeOperation) -> dict:
        """Returns the whole attributes kwargs for a generic Operation node

        Args:
            node (GraphNodeOperation): input node

        Returns:
            dict: attributes for the node
        """
        return {
            **(self._operation_styles(node)),
            "shape": self._operation_shape(node),
            "label": self._operation_node_content(node),
            # Easter egg
            "URL": "https://c.tenor.com/2jvzfQQxDFcAAAAC/think-smart.gif",
        }

    def _data_attributes(self, node: GraphNodeData) -> dict:
        """Returns the whole attributes kwargs for a generic Data node

        Args:
            node (GraphNodeData): input node

        Returns:
            dict: attributes for the node
        """
        return {
            **(self._data_styles(node)),
            "shape": self._data_shape(node),
            "label": self._data_node_content(node),
        }

    def _operation_node_content(self, node: GraphNodeOperation) -> str:
        """Returns the textual representation of an Operation node in Graphviz TABLE
        format

        Args:
            node (GraphNodeOperation): input node

        Returns:
            str: textual representation of the node
        """
        content = '<<TABLE BORDER="0" CELLBORDER="0" CELLPADDING="20">'
        content += "<TR>"
        content += f"<TD>{node.node_model.command}</TD>"
        content += "</TR>"
        content += "</TABLE>>"
        return content

    def _data_node_content(self, node: GraphNodeData) -> str:
        """Returns the textual representation of a Data node in Graphviz TABLE format

        Args:
            node (GraphNodeData): input node

        Returns:
            str: textual representation of the node
        """
        content = '<<TABLE BORDER="0" CELLBORDER="0" CELLPADDING="20">'
        content += "<TR>"
        content += f"<TD>{node.path}</TD>"
        content += "</TR>"
        content += "</TABLE>>"
        return content

    def _add_node_to_agraph(self, agraph: pgv.AGraph, node: GraphNode):
        """Adds a node to the graph distinguising between operation and data nodes

        Args:
            agraph (pgv.AGraph): graph to add the node to
            node (GraphNode): node to add
        """
        if isinstance(node, GraphNodeOperation):
            agraph.add_node(node, **(self._operation_attributes(node)))
        elif isinstance(node, GraphNodeData):
            agraph.add_node(node, **(self._data_attributes(node)))

    def _data_edge_style(self, node: GraphNodeData, edge_attrs: dict) -> dict:
        """Returns the style attributes for a data edge (an edge starting from a Data
        node and ending in an Operation node)

        Args:
            node (GraphNodeData): input node
            edge_attrs (dict): edge attributes

        Returns:
            dict: style attributes for the edge
        """
        return {
            "color": "#1DE9B6",
            "arrowsize": 2.0,
            "style": "dashed",
        }

    def _operation_edge_style(self, node: GraphNodeOperation, edge_attrs: dict) -> dict:
        """Returns the style attributes for an operation edge (an edge starting from an
        Operation node and ending in a Data node)

        Args:
            node (GraphNodeOperation): input node
            edge_attrs (dict): edge attributes

        Returns:
            dict: style attributes for the edge
        """
        return {
            "color": "#00B0FF",
            "arrowsize": 1.0,
        }

    def _add_edge_op2data(
        self,
        agraph: pgv.AGraph,
        n0: GraphNodeOperation,
        n1: GraphNodeData,
        edge_attrs: dict,
    ):
        """Adds an edge from an Operation node to a Data node

        Args:
            agraph (pgv.AGraph): graph to add the edge to
            n0 (GraphNodeOperation): starting node
            n1 (GraphNodeData): ending node
            edge_attrs (dict): edge attributes
        """

        edge_name = edge_attrs.get(DAGNodesGraph.GraphAttrs.OUTPUT_PORT, "")
        agraph.add_edge(
            n0,
            n1,
            label=f"{edge_name}",
            **(self._data_edge_style(n1, edge_attrs)),
        )

    def _add_edge_data2op(
        self,
        agraph: pgv.AGraph,
        n0: GraphNodeData,
        n1: GraphNodeOperation,
        edge_attrs: dict,
    ):
        """Adds an edge from a Data node to an Operation node

        Args:
            agraph (pgv.AGraph): graph to add the edge to
            n0 (GraphNodeData): starting node
            n1 (GraphNodeOperation): ending node
            edge_attrs (dict): edge attributes
        """

        edge_name = edge_attrs.get(DAGNodesGraph.GraphAttrs.INPUT_PORT, "")
        agraph.add_edge(
            n0,
            n1,
            label=f"{edge_name}",
            **(self._operation_edge_style(n1, edge_attrs)),
        )

    def _add_edge_to_agraph(
        self,
        agraph: pgv.AGraph,
        n0: GraphNode,
        n1: GraphNode,
        edge_attrs: dict,
    ):
        """Adds an edge to the graph

        Args:
            agraph (pgv.AGraph): graph to add the edge to
            n0 (GraphNode): starting node
            n1 (GraphNode): ending node
            edge_attrs (dict): edge attributes

        Raises:
            ValueError: if the edge is not managed
        """

        if isinstance(n0, GraphNodeOperation) and isinstance(n1, GraphNodeData):
            self._add_edge_op2data(agraph, n0, n1, edge_attrs)
        elif isinstance(n0, GraphNodeData) and isinstance(n1, GraphNodeOperation):
            self._add_edge_data2op(agraph, n0, n1, edge_attrs)
        else:
            raise ValueError(f"Unknown edge type: {type(n0)}, {type(n1)}")

    def _build_agraph(self, graph: DAGNodesGraph) -> pgv.AGraph:
        """Builds the graphviz graph from a DAGNodesGraph

        Args:
            graph (DAGNodesGraph): input graph

        Returns:
            pgv.AGraph: graphviz graph
        """
        agraph = pgv.AGraph(directed=True)

        for edge in graph.raw_graph.edges():
            n0: GraphNode = edge[0]
            n1: GraphNode = edge[1]

            self._add_node_to_agraph(agraph, n0)
            self._add_node_to_agraph(agraph, n1)
            self._add_edge_to_agraph(agraph, n0, n1, graph.raw_graph.edges[n0, n1])

        return agraph

    def draw(self, graph: DAGNodesGraph) -> np.ndarray:
        """Draws a graph and returns the image as a numpy array

        Args:
            graph (DAGNodesGraph): graph to draw

        Returns:
            np.ndarray: image as a numpy array
        """
        import imageio

        agraph: pgv.AGraph = self._build_agraph(graph)
        agraph.layout("dot")
        f = NamedTemporaryFile(suffix=".png")
        agraph.draw(f.name)
        return imageio.imread(f.name)

    def representation(self, graph: DAGNodesGraph) -> str:
        """Returns a representation of the graph as a DOT string

        Args:
            graph (DAGNodesGraph): graph to represent

        Returns:
            str: DOT string
        """

        agraph = self._build_agraph(graph)

        return agraph.string()

    def exportable_formats(self) -> Sequence[str]:
        """Returns the exportable formats. They are mixed formats (png, pdf, ...) and
        the dot format.

        Returns:
            Sequence[str]: exportable formats
        """
        return ["png", "dot", "pdf", "svg"]

    def export(self, graph: DAGNodesGraph, filename: str, format: Optional[str] = None):
        """Exports the graph to a file

        Args:
            graph (DAGNodesGraph): graph to export
            filename (str): filename
            format (str): format
        """
        super().export(graph, filename, format)

        if format is None:
            format = Path(filename).suffix[1:]

        agraph = self._build_agraph(graph)
        if format in ["dot"]:
            agraph.write(filename)
        else:
            agraph.layout("dot")
            agraph.draw(filename)
