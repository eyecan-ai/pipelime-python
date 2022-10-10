import typing as t
from enum import Enum
from pathlib import Path

from pydantic import Field, PositiveInt

from pipelime.piper import PipelimeCommand, PiperPortType


def _get_command(cmd) -> PipelimeCommand:
    from pipelime.cli.utils import PipelimeSymbolsHelper

    if isinstance(cmd, PipelimeCommand):
        return cmd

    cmd_name, cmd_args = next(iter(cmd.items()))

    cmd_cls = PipelimeSymbolsHelper.get_command(cmd_name)
    if cmd_cls is None or not issubclass(cmd_cls[1], PipelimeCommand):
        PipelimeSymbolsHelper.show_error_and_help(
            cmd_name, should_be_cmd=True, should_be_op=False, should_be_stage=False
        )
        raise ValueError(f"{cmd_name} is not a pipelime command.")
    cmd_cls = cmd_cls[1]
    return cmd_cls() if cmd_args is None else cmd_cls(**cmd_args)


class RunCommand(PipelimeCommand, title="run"):
    """Executes a DAG of pipelime commands."""

    nodes: t.Mapping[
        str, t.Union[PipelimeCommand, t.Mapping[str, t.Optional[t.Mapping[str, t.Any]]]]
    ] = Field(
        ...,
        alias="n",
        description=(
            "A DAG of commands as a `<node>: <command>` mapping. The command can be a "
            "`<name>: <args>` mapping, where `<name>` is `pipe`, `clone`, `split` etc, "
            "while `<args>` is a mapping of its arguments."
        ),
        piper_port=PiperPortType.INPUT,
    )
    include: t.Union[str, t.Sequence[str], None] = Field(
        None, alias="i", description="Nodes not in this list are not run."
    )
    exclude: t.Union[str, t.Sequence[str], None] = Field(
        None, alias="e", description="Nodes in this list are not run."
    )
    token: t.Optional[str] = Field(
        None,
        alias="t",
        description=(
            "The execution token. If not specified, a new token will be generated."
        ),
    )
    watch: bool = Field(
        True, alias="w", description="Monitor the execution in the current console."
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

        inc_n = [self.include] if isinstance(self.include, str) else self.include
        exc_n = [self.exclude] if isinstance(self.exclude, str) else self.exclude

        def _node_to_run(node: str) -> bool:
            return (inc_n is None or node in inc_n) and (
                exc_n is None or node not in exc_n
            )

        nodes = {
            name: _get_command(cmd)
            for name, cmd in self.nodes.items()
            if _node_to_run(name)
        }
        dag = DAGModel(nodes=nodes)
        graph = DAGNodesGraph.build_nodes_graph(dag)
        executor = NodesGraphExecutorFactory.get_executor(watch=self.watch)
        self.successful = executor.exec(graph, token=self.token)


class DrawCommand(PipelimeCommand, title="draw"):
    """Draws a pipelime DAG."""

    class DrawBackendChoice(Enum):
        GRAPHVIZ = "graphviz"
        MERMAID = "mermaid"

    class EllipsesChoice(Enum):
        START = "start"
        MIDDLE = "middle"
        END = "end"

    nodes: t.Mapping[
        str, t.Union[PipelimeCommand, t.Mapping[str, t.Optional[t.Mapping[str, t.Any]]]]
    ] = Field(
        ...,
        alias="n",
        description=(
            "A DAG of commands as a `<node>: <command>` mapping. The command can be a "
            "`<name>: <args>` mapping, where `<name>` is `pipe`, `clone`, `split` etc, "
            "while `<args>` is a mapping of its arguments."
        ),
        piper_port=PiperPortType.INPUT,
    )
    include: t.Union[str, t.Sequence[str], None] = Field(
        None, alias="i", description="Nodes not in this list are not drawn."
    )
    exclude: t.Union[str, t.Sequence[str], None] = Field(
        None, alias="e", description="Nodes in this list are not drawn."
    )
    output: t.Optional[Path] = Field(
        None,
        alias="o",
        description=(
            "The output file. If not specified, the graph will be shown in a window."
        ),
        piper_port=PiperPortType.OUTPUT,
    )
    raw_output: t.Optional[str] = Field(
        None,
        description="Raw graph representation",
        exclude=True,
        repr=False,
        piper_port=PiperPortType.OUTPUT,
    )
    backend: DrawBackendChoice = Field(
        DrawBackendChoice.GRAPHVIZ, alias="b", description="The graph backend to use."
    )
    open: bool = Field(
        False,
        description=(
            "If `output` has been set, open the image file in the default viewer."
        ),
    )

    data_max_width: t.Union[PositiveInt, str, None] = Field(
        None,
        alias="m",
        description=(
            "If an int is given, it is the maximum data node name length. "
            "If a string is given, it is matched against the node name and "
            "replaced with ellipses. Note that the search starts from "
            "the last character if `ellipsis_position` is `start`."
        ),
    )
    ellipsis_position: EllipsesChoice = Field(
        EllipsesChoice.MIDDLE,
        alias="ep",
        description="Where ellipses should be put if the data node name is too long.",
    )
    show_command_names: bool = Field(
        False, alias="c", description="Show command names instead of node names."
    )
    extra_args: t.Optional[t.Mapping[str, t.Any]] = Field(
        None, alias="x", description="Extra arguments to pass to the backend."
    )
    raw: bool = Field(
        False, alias="r", description="Show the raw graph representation."
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

        inc_n = [self.include] if isinstance(self.include, str) else self.include
        exc_n = [self.exclude] if isinstance(self.exclude, str) else self.exclude

        def _node_to_draw(node: str) -> bool:
            return (inc_n is None or node in inc_n) and (
                exc_n is None or node not in exc_n
            )

        nodes = {
            name: _get_command(cmd)
            for name, cmd in self.nodes.items()
            if _node_to_draw(name)
        }
        dag = DAGModel(nodes=nodes)
        graph = DAGNodesGraph.build_nodes_graph(
            dag,
            data_max_width=self.data_max_width,
            show_command_name=self.show_command_names,
            ellipsis_position=self.ellipsis_position.value,
        )
        drawer = NodesGraphDrawerFactory.create(self.backend.value)

        extra = self.extra_args or {}

        # raw graph representation
        if self.raw:
            self.raw_output = drawer.representation(graph)
        else:
            # Show or Write
            if self.output is not None:
                drawer.export(graph, str(self.output), **extra)
                if self.open:
                    start_file(str(self.output))
            else:
                from PIL import Image

                graph_image = drawer.draw(graph=graph, **extra)
                img = Image.fromarray(graph_image, "RGB")
                img.show("Graph")
