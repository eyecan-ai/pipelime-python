import typing as t
from enum import Enum
from pathlib import Path

from pydantic import Field

from pipelime.piper import PipelimeCommand, PiperPortType


class PiperRunCommand(PipelimeCommand, title="piper-run"):
    """Executes a Piper DAG."""

    nodes: t.Mapping[str, PipelimeCommand] = Field(
        ...,
        description="A Piper DAG as a `<node>: <command>` mapping.",
        piper_port=PiperPortType.INPUT,
    )
    token: t.Optional[str] = Field(
        None,
        description=(
            "The piper execution token. "
            "If not specified, a new token will be generated."
        ),
    )
    watch: bool = Field(
        True, description="Monitor the execution in the current console."
    )
    successful: bool = Field(
        None,
        description="True if the execution was successful",
        exclude=True,
        repr=False,
        piper_port=PiperPortType.OUTPUT,
    )

    def run(self):
        import uuid

        from pipelime.piper.executors.factory import NodesGraphExecutorFactory
        from pipelime.piper.graph import DAGNodesGraph
        from pipelime.piper.model import DAGModel

        if not self.token:
            self.token = uuid.uuid1().hex

        dag = DAGModel(nodes=self.nodes)
        graph = DAGNodesGraph.build_nodes_graph(dag)
        executor = NodesGraphExecutorFactory.get_executor(watch=self.watch)
        self.successful = executor.exec(graph, token=self.token)


class PiperDrawCommand(PipelimeCommand, title="piper-draw"):
    """Draws a piper DAG."""

    class DrawBackendChoice(Enum):
        GRAPHVIZ = "graphviz"
        MERMAID = "mermaid"

    nodes: t.Mapping[str, PipelimeCommand] = Field(
        ...,
        description="A Piper DAG as a `<node>: <command>` mapping.",
        piper_port=PiperPortType.INPUT,
    )
    output: t.Optional[Path] = Field(
        None,
        description=(
            "The output file. If not specified, the graph will be shown in a window."
        ),
        piper_port=PiperPortType.OUTPUT,
    )
    backend: DrawBackendChoice = Field(
        DrawBackendChoice.GRAPHVIZ, description="The graph backend to use."
    )
    open: bool = Field(
        False,
        description=(
            "If `output` has been set, open the image file in the default viewer."
        ),
    )

    def run(self):
        import os
        import platform
        import subprocess

        from pipelime.piper.drawing.factory import NodesGraphDrawerFactory
        from pipelime.piper.graph import DAGNodesGraph
        from pipelime.piper.model import DAGModel

        def start_file(filename: str):
            if platform.system() == "Darwin":  # macOS
                subprocess.call(("open", filename))
            elif platform.system() == "Windows":  # Windows
                os.startfile(filename)
            else:  # linux variants #TODO: verify!
                subprocess.call(("xdg-open", filename))

        dag = DAGModel(nodes=self.nodes)
        graph = DAGNodesGraph.build_nodes_graph(dag)
        drawer = NodesGraphDrawerFactory.create(self.backend.value)

        # Show or Write
        if self.output is not None:
            drawer.export(graph, str(self.output))
            if self.open:
                start_file(str(self.output))
        else:
            from PIL import Image

            graph_image = drawer.draw(graph=graph)
            img = Image.fromarray(graph_image, "RGB")
            img.show("Graph")
