import base64
from io import BytesIO
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import requests
from PIL import Image

from pipelime.piper.drawing.base import NodesGraphDrawer
from pipelime.piper.graph import (
    DAGNodesGraph,
    GraphNode,
    GraphNodeData,
    GraphNodeOperation,
)


class MermaidNodesGraphDrawer(NodesGraphDrawer):  # pragma: no cover
    PRIVATE_CHARS = ["@"]
    SUBSTITUTION_CHAR = "_"
    OPERATION_NAME = "operation"
    DATA_NAME = "data"
    MERMAID_SERVICE_URL = "https://mermaid.ink/img/{}"

    def _is_node_file(self, node: GraphNodeData) -> bool:
        """Returns True if the node path is (or could be) a file

        Args:
            node (GraphNodeData):  input node

        Returns:
            bool:  True if the node path is (or could be) a file
        """
        return len(Path(node.path).suffix) > 0

    def _data_node_shape(self, node: GraphNodeData) -> str:
        """Returns the shape string of a data node

        Args:
            node (GraphNodeData): input node

        Returns:
            str: shape of the node
        """

        name = node.readable_repr
        sep = ("[", "]") if self._is_node_file(node) else ("(", ")")
        return f"{name}[{sep[0]}{name}{sep[1]}]"

    def _node_representation(self, node: GraphNode) -> str:
        """Returns the string mermaid representation of a node

        Args:
            node (GraphNode): node to represent

        Raises:
            ValueError: if node is not a valid node

        Returns:
            str: mermaid textual representation of the node
        """

        out = ""
        if isinstance(node, GraphNodeOperation):
            out = f"{node.readable_repr}:::{MermaidNodesGraphDrawer.OPERATION_NAME}"
        elif isinstance(node, GraphNodeData):
            out = f"{self._data_node_shape(node)}:::{MermaidNodesGraphDrawer.DATA_NAME}"
        else:
            raise ValueError(f"Unknown node type: {type(node)}")

        for c in MermaidNodesGraphDrawer.PRIVATE_CHARS:
            out = out.replace(c, MermaidNodesGraphDrawer.SUBSTITUTION_CHAR)
        return out

    def _style_attrs_operation(self) -> dict:
        """
        Returns the style attributes for an operation node
        """
        return {"fill": "#03A9F4", "stroke": "#fafafa", "color": "#fff"}

    def _style_attrs_data(self) -> dict:
        """
        Returns the style attributes for a data node
        """
        return {"fill": "#009688", "stroke": "#fafafa", "color": "#fff"}

    def _classdef_row(self, name: str, style_attrs: dict) -> str:
        """Returns the string representation of a class (style) definition

        Args:
            name (str): name of the class
            style_attrs (dict): style attributes of the class

        Returns:
            str: string representation of the class definition
        """

        def style_string(x):
            return ",".join([f"{k}:{v}" for k, v in x.items()])

        return f"classDef {name} {style_string(style_attrs)};"

    def _header_row(self, orientation: str = "TB") -> str:
        """Returns the string representation of the header row of the mermaid graph

        Args:
            orientation (str, optional): TB - top to bottom, TD - top-down/ same as top
            to bottom, BT - bottom to top, RL - right to left, LR - left to right .
            Defaults to "TB".

        Returns:
            str: string representation of the header row
        """
        return f"flowchart {orientation}"

    def _edge_row(self, graph: DAGNodesGraph, n0: GraphNode, n1: GraphNode) -> str:
        """Returns the string representation of an edge row in the mermaid graph

        Args:
            graph (DAGNodesGraph): graph to draw
            n0 (GraphNode): first node of the edge
            n1 (GraphNode): second node of the edge

        Returns:
            str: string representation of the edge row
        """

        edge_attrs = graph.raw_graph.edges[n0, n1]

        edge_name = ""
        if DAGNodesGraph.GraphAttrs.INPUT_PORT in edge_attrs:
            edge_name = f"|{edge_attrs[DAGNodesGraph.GraphAttrs.INPUT_PORT]}|"
        if DAGNodesGraph.GraphAttrs.OUTPUT_PORT in edge_attrs:
            edge_name += f"|{edge_attrs[DAGNodesGraph.GraphAttrs.OUTPUT_PORT]}|"

        return "{}-->{}{}".format(
            self._node_representation(n0),
            edge_name,
            self._node_representation(n1),
        )

    def _add_row(self, representation: str, row: str, header: bool = False):
        """Append a row to the representation with correct indentation and newline

        Args:
            representation (str): representation to append to
            row (str): row to append
            header (bool, optional): if TRUE the row is a header row. Defaults to False.

        Returns:
            _type_: representation with the row appended
        """
        representation = representation + (f"{row}\n" if header else f"\t{row}\n")
        return representation

    def draw(self, graph: DAGNodesGraph, **kwargs) -> np.ndarray:
        """Draws a graph and returns the image as a numpy array

        Args:
            graph (DAGNodesGraph): graph to draw

        Returns:
            np.ndarray: image as a numpy array
        """

        representation = self.representation(graph)
        graphbytes = representation.encode("ascii")
        base64_bytes = base64.b64encode(graphbytes)
        base64_string = base64_bytes.decode("ascii")
        url = MermaidNodesGraphDrawer.MERMAID_SERVICE_URL.format(base64_string)
        response = requests.get(url)
        return np.array(Image.open(BytesIO(response.content)))

    def representation(self, graph: DAGNodesGraph) -> str:
        """Returns the string representation of the graph in the mermaid language

        Args:
            graph (DAGNodesGraph): graph to draw

        Returns:
            str: string representation of the graph in the mermaid language
        """

        # execution_stack = graph.build_execution_stack()

        # initialize representation
        repr = ""

        # flochart header
        repr = self._add_row(repr, self._header_row(), header=True)

        # style calss for operations
        repr = self._add_row(
            repr,
            self._classdef_row(self.OPERATION_NAME, self._style_attrs_operation()),
        )

        # style class for data
        repr = self._add_row(
            repr,
            self._classdef_row(self.DATA_NAME, self._style_attrs_data()),
        )

        for edge in graph.raw_graph.edges():
            n0: GraphNode = edge[0]
            n1: GraphNode = edge[1]

            repr = self._add_row(repr, self._edge_row(graph, n0, n1))

        return repr

    def exportable_formats(self) -> Sequence[str]:
        """Returns the exportable formats of the graph drawer.

        Returns:
            Sequence[str]: exportable formats of the graph drawer
        """
        return ["png", "markdown", "md"]

    def export(
        self,
        graph: DAGNodesGraph,
        filename: str,
        format: Optional[str] = None,
        **kwargs,
    ):
        format = self._check_format(filename, format)

        if format == "png":
            import imageio

            img = self.draw(graph, **kwargs)
            imageio.imwrite(filename, img)
        elif format in ["markdown", "md"]:
            representation = self.representation(graph)
            markdown = f"\n```mermaid\n{representation}\n```\n"
            with open(filename, "w") as f:
                f.write(markdown)
