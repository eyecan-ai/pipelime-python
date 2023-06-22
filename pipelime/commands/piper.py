import typing as t
from abc import abstractmethod
from enum import Enum
from pathlib import Path

from loguru import logger
from pydantic import Field, PositiveInt, PrivateAttr, validator

from pipelime.choixe.utils.io import PipelimeTemporaryDirectory
from pipelime.piper import PipelimeCommand, PiperPortType

if t.TYPE_CHECKING:
    from pipelime.piper.graph import DAGNodesGraph

T_NODES = t.Mapping[
    str,
    t.Union[PipelimeCommand, t.Mapping[str, t.Optional[t.Mapping[str, t.Any]]]],
]


class WatcherBackend(Enum):
    """The watcher backend to use."""

    RICH = "rich"
    TQDM = "tqdm"

    def listener_key(self) -> str:
        return {
            WatcherBackend.RICH: "RICH_TABLE",
            WatcherBackend.TQDM: "TQDM_BARS",
        }[self]


class PiperGraphCommandBase(PipelimeCommand):
    """Base class for piper-aware commands."""

    nodes: T_NODES = Field(
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

    _piper_graph: t.Optional["DAGNodesGraph"] = PrivateAttr(None)

    @property
    def piper_graph(self) -> "DAGNodesGraph":
        from pipelime.piper.graph import DAGNodesGraph
        from pipelime.piper.model import DAGModel, NodesDefinition

        if not self._piper_graph:
            inc_n = [self.include] if isinstance(self.include, str) else self.include
            exc_n = [self.exclude] if isinstance(self.exclude, str) else self.exclude

            def _node_to_run(node: str) -> bool:
                return (inc_n is None or node in inc_n) and (
                    exc_n is None or node not in exc_n
                )

            nodes = {
                name: cmd for name, cmd in self.nodes.items() if _node_to_run(name)
            }
            dag = DAGModel(nodes=NodesDefinition.create(nodes))
            self._piper_graph = DAGNodesGraph.build_nodes_graph(
                dag, **self._nodes_graph_building_kwargs()
            )
        return self._piper_graph

    def _nodes_graph_building_kwargs(self) -> t.Mapping[str, t.Any]:
        """Subclasses should returns the extra kwargs options for
        ``DAGNodesGraph.build_nodes_graph``
        """
        return {}


class GraphPortForwardingCommand(PiperGraphCommandBase):
    """A command that uses the root and leaf data nodes of the internal DAG
    as its own I/O ports."""

    def get_inputs(self) -> t.Dict[str, t.Any]:
        return {
            self.piper_graph.get_input_port_name(x): x.path
            for x in self.piper_graph.input_data_nodes
        }

    def get_outputs(self) -> t.Dict[str, t.Any]:
        return {
            self.piper_graph.get_output_port_name(x): x.path
            for x in self.piper_graph.output_data_nodes
        }


class RunCommand(GraphPortForwardingCommand, title="run"):
    """Executes a DAG of pipelime commands.
    NB: when run inside a graph, `token` and `watch` are ignored."""

    token: t.Optional[str] = Field(
        None,
        alias="t",
        description=(
            "The execution token. If not specified, a new token will be generated."
        ),
    )
    watch: t.Union[bool, WatcherBackend, Path, None] = Field(
        None,
        alias="w",
        description=(
            "Monitor the execution in the current console. "
            "Defaults to True if no token is provided, False othrewise. "
            "If a string is provided, it is used as the name of the watcher backend or "
            "the json/yaml progress file path."
        ),
    )
    force_gc: t.Union[bool, str, t.Sequence[str]] = Field(
        False,
        alias="gc",
        description=(
            "Force garbage collection before and after the execution of all nodes, "
            "if True, or only for the specified nodes."
        ),
    )

    def run(self):
        import uuid
        from contextlib import ExitStack

        from pipelime.piper.executors.factory import NodesGraphExecutorFactory

        exit_stack = ExitStack()

        if self._piper.active:
            # nested graph should disable the default watcher
            # and forward the token of the parent graph
            watch = False
            token = self._piper.token
            message = f"{self._piper.node} DAG"
            prefix = f"{self._piper.node}."
        else:
            watch = not self.token if self.watch is None else self.watch
            token = self.token or uuid.uuid1().hex
            message = "Main DAG"
            prefix = ""

            # activate piper, so that this node will send updates to the watchers
            self.set_piper_info(token=token, node=message)

        # setup the direct track callback
        if watch:
            from pipelime.piper.progress.listener.factory import ListenerCallbackFactory
            from pipelime.piper.progress.tracker.direct import DirectTrackCallback

            if isinstance(watch, Path):
                callback = ListenerCallbackFactory.get_callback("FILE", filename=watch)
            else:
                callback = ListenerCallbackFactory.get_callback(
                    watch.listener_key()
                    if isinstance(watch, WatcherBackend)
                    else ListenerCallbackFactory.DEFAULT_CALLBACK_TYPE
                )

            PipelimeCommand._track_callback = DirectTrackCallback(callback)

            # disable annoying logging
            logger.disable("pipelime")
            exit_stack.callback(logger.enable, "pipelime")
            exit_stack.callback(PipelimeCommand._track_callback.stop_callbacks)

        with exit_stack:
            executor = NodesGraphExecutorFactory.get_executor(
                watch=False,
                node_prefix=prefix,
                task=self.create_task(
                    total=self.piper_graph.num_operation_nodes, message=message
                ),
            )

            if not executor(self.piper_graph, token=token, force_gc=self.force_gc):
                raise RuntimeError("Piper execution failed")


class DrawCommand(PiperGraphCommandBase, title="draw"):
    """Draws a pipelime DAG."""

    class DrawBackendChoice(Enum):
        AUTO = "auto"
        GRAPHVIZ = "graphviz"
        MERMAID = "mermaid"

    class EllipsesChoice(Enum):
        START = "start"
        MIDDLE = "middle"
        END = "end"
        REGEX = "regex"

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
        DrawBackendChoice.AUTO, alias="b", description="The graph backend to use."
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

    def _nodes_graph_building_kwargs(self) -> t.Mapping[str, t.Any]:
        return {
            "data_max_width": self.data_max_width,
            "show_command_name": self.show_command_names,
            "ellipsis_position": self.ellipsis_position.value,
        }

    def run(self):
        import os
        import platform
        import subprocess

        from pipelime.piper.drawing.factory import NodesGraphDrawerFactory

        def start_file(filename: str):
            if platform.system() == "Darwin":  # macOS
                subprocess.call(("open", filename))
            elif platform.system() == "Windows":  # Windows
                os.startfile(filename)
            else:  # linux variants #TODO: verify!
                subprocess.call(("xdg-open", filename))

        if self.backend == self.DrawBackendChoice.AUTO:
            try:
                import pygraphviz  # noqa: F401

                self.backend = self.DrawBackendChoice.GRAPHVIZ
            except ImportError:
                self.backend = self.DrawBackendChoice.MERMAID

        graph = self.piper_graph
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


class WatchCommand(PipelimeCommand, title="watch"):
    """Show the progress of a running DAG."""

    token: t.Optional[str] = Field(
        None,
        alias="t",
        description=(
            "The token of the DAG you want to monitor. "
            "Defaults to any DAG if not specified."
        ),
    )
    watcher: t.Union[WatcherBackend, Path] = Field(
        WatcherBackend.TQDM,
        alias="w",
        description=(
            "The watcher to use. If a string is provided, it is used as the name "
            "of the backend or the progress file path."
        ),
    )
    port: t.Optional[int] = Field(
        None,
        alias="p",
        description="The port to listen to. Use the default port if not specified.",
    )

    def run(self):
        from time import sleep

        from pipelime.piper.progress.listener.base import Listener
        from pipelime.piper.progress.listener.factory import (
            ListenerCallbackFactory,
            ProgressReceiverFactory,
        )
        from pipelime.utils.context_managers import CatchSignals

        receiver = ProgressReceiverFactory.get_receiver(
            self.token, **({"port": self.port} if self.port else {})
        )
        if isinstance(self.watcher, WatcherBackend):
            callback = ListenerCallbackFactory.get_callback(
                self.watcher.listener_key(), show_token=self.token is None
            )
        else:
            callback = ListenerCallbackFactory.get_callback(
                "FILE", filename=self.watcher, show_token=self.token is None
            )
        listener = Listener(receiver, callback)
        listener.start()

        with CatchSignals() as catcher:
            while not catcher.interrupted:
                sleep(0.1)

        listener.stop()


class DagBaseCommand(RunCommand):
    """Base class for Python DAG Object."""

    nodes: t.Optional[T_NODES] = Field(
        None,
        description="A DAG of commands as a `<node>: <command>` mapping. The command "
        "can be a `<name>: <args>` mapping, where `<name>` is `pipe`, `clone`, `split`"
        "etc, while `<args>` is a mapping of its arguments.",
    )

    folder_debug: Path = Field(None, description="Path to Debug dir folder.")

    _temp_folder: PipelimeTemporaryDirectory = PrivateAttr(None)

    def __init__(self, **data):
        super().__init__(**data)

        self._temp_folder = PipelimeTemporaryDirectory(self.folder_debug)  # type: ignore
        self.folder_debug = self._temp_folder.name

    def _validate_graph(self):
        """Validates the graph before executing it.

        Raises:
            RuntimeError: if cycle is found.
        """
        from networkx.algorithms.cycles import find_cycle
        from networkx.exception import NetworkXNoCycle

        try:
            edges_cycles = find_cycle(self.piper_graph.raw_graph)
        except NetworkXNoCycle:
            return

        raise RuntimeError(f"Cycle found {edges_cycles}")  # type: ignore

    def draw(self, output: t.Optional[Path] = None) -> None:
        """Draws a pipelime DAG.

        Args:
            output (Path, optional): The output file. If not specified, the graph will
            be shown in a window. Defaults to None.
        """
        nodes = self.nodes if self.nodes else self.create_graph()
        drawer = DrawCommand(nodes=nodes, output=output)  # type: ignore
        drawer.run()

    @abstractmethod
    def create_graph(self) -> T_NODES:
        """Creates the graph nodes.

        Returns:
            T_NODES: a dictionary containing the mapping between node names and nodes.
        """
        pass

    def run(self) -> None:
        self.nodes = self.create_graph()

        self._validate_graph()

        return super().run()
