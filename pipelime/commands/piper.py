import typing as t
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path

from loguru import logger
from pydantic import BaseModel, Field, PositiveInt, PrivateAttr, create_model, validator

from pipelime.piper.model import T_NODES, LazyCommand, PipelimeCommand, PiperPortType

if t.TYPE_CHECKING:
    from pipelime.piper.checkpoint import CheckpointNamespace
    from pipelime.piper.graph import DAGNodesGraph


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

    include: t.Union[str, t.Sequence[str], None] = Field(
        None, alias="i", description="Nodes not in this list are not created."
    )
    exclude: t.Union[str, t.Sequence[str], None] = Field(
        None, alias="e", description="Nodes in this list are not created."
    )
    skip_on_error: bool = Field(
        False, alias="se", description="Skip commands on validation errors."
    )
    start_from: t.Union[str, t.Sequence[str], None] = Field(
        None,
        alias="sf",
        description=(
            "Create the graph, then take only these nodes and their descendants."
        ),
    )
    stop_at: t.Union[str, t.Sequence[str], None] = Field(
        None,
        alias="sa",
        description="Create the graph, then take only these nodes and their ancestors.",
    )

    _piper_graph: t.Optional["DAGNodesGraph"] = PrivateAttr(None)

    @classmethod
    def init_from_checkpoint(cls, checkpoint: "CheckpointNamespace", /, **data):
        """Derived classes may override to support command resuming."""

        # user may override the include/exclude lists when resuming
        if (
            "include" not in data
            and "i" not in data
            and "exclude" not in data
            and "e" not in data
        ):
            ckpt_data = checkpoint.read_data("include", False)
            if ckpt_data is not False:
                data["include"] = ckpt_data
                logger.debug(
                    f"[{checkpoint.namespace}] Restoring included nodes "
                    f"from checkpoint: {ckpt_data}"
                )

            ckpt_data = checkpoint.read_data("exclude", False)
            if ckpt_data is not False:
                data["exclude"] = ckpt_data
                logger.debug(
                    f"[{checkpoint.namespace}] Restoring excluded nodes "
                    f"from checkpoint: {ckpt_data}"
                )

        return cls(**data)

    @property
    @abstractmethod
    def nodes_graph(self) -> T_NODES:
        """The DAG of commands as a `<node>: <command>` mapping."""

    @property
    def piper_graph(self) -> "DAGNodesGraph":
        from pipelime.piper.graph import DAGNodesGraph
        from pipelime.piper.model import DAGModel, NodesDefinition
        import fnmatch

        if not self._piper_graph:
            inc_n = [self.include] if isinstance(self.include, str) else self.include
            exc_n = [self.exclude] if isinstance(self.exclude, str) else self.exclude

            self.command_checkpoint.write_data("include", inc_n)
            self.command_checkpoint.write_data("exclude", exc_n)

            def _list_match(node: str, patterns: t.Sequence[str]) -> bool:
                for pattern in patterns:
                    if fnmatch.fnmatchcase(node, pattern):
                        return True
                return False

            def _node_to_run(node: str) -> bool:
                return (inc_n is None or _list_match(node, inc_n)) and (
                    exc_n is None or not _list_match(node, exc_n)
                )

            nodes = {
                name: cmd
                for name, cmd in self.nodes_graph.items()
                if _node_to_run(name)
            }
            dag = DAGModel(
                nodes=NodesDefinition.create(
                    nodes,
                    checkpoint=self.command_checkpoint,
                    skip_on_error=self.skip_on_error,
                )
            )
            self._piper_graph = DAGNodesGraph.build_nodes_graph(
                dag, **self._nodes_graph_building_kwargs()
            )

            if self.start_from is not None:
                self._piper_graph = self._piper_graph.start_from_nodes(
                    [self.start_from]
                    if isinstance(self.start_from, str)
                    else self.start_from
                )
            if self.stop_at is not None:
                self._piper_graph = self._piper_graph.stop_at_nodes(
                    [self.stop_at] if isinstance(self.stop_at, str) else self.stop_at
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

    @property
    def input_mapping(self) -> t.Optional[t.Mapping[str, str]]:
        """Optional mapping from graph input data keys and desired keys."""
        return None

    @property
    def output_mapping(self) -> t.Optional[t.Mapping[str, str]]:
        """Optional mapping from graph output data keys and desired keys."""
        return None

    def _mapped_name(self, name: str, mapping: t.Optional[t.Mapping[str, str]]) -> str:
        return mapping.get(name, "") if mapping else name

    def get_inputs(self) -> t.Dict[str, t.Any]:
        inmap = self.input_mapping
        return {
            self._mapped_name(self.piper_graph.get_input_port_name(x), inmap): x.path
            for x in self.piper_graph.input_data_nodes
        }

    def get_outputs(self) -> t.Dict[str, t.Any]:
        outmap = self.output_mapping
        return {
            self._mapped_name(self.piper_graph.get_output_port_name(x), outmap): x.path
            for x in self.piper_graph.output_data_nodes
        }


class RunCommandBase(GraphPortForwardingCommand):
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
            executor.add_post_callback(self._update_completed_commands)

            if not executor(self.piper_graph, token=token, force_gc=self.force_gc):
                raise RuntimeError("Piper execution failed")

    def _update_completed_commands(self, name: str, command: PipelimeCommand):
        """Updates the exclude lists after a command has been executed."""
        with self.command_checkpoint.create_lock() as lock:
            exc = self.command_checkpoint.read_data("exclude", None, lock)
            if exc is None:
                exc = []
            exc.append(name)
            self.command_checkpoint.write_data("exclude", exc, lock)


class ClassicPiperGraphCommand(BaseModel):
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

    @property
    def nodes_graph(self) -> T_NODES:
        return self.nodes


class RunCommand(ClassicPiperGraphCommand, RunCommandBase, title="run"):
    """Executes a DAG of pipelime commands.
    NB: when run inside a graph, `token` and `watch` are ignored."""


class DrawCommand(ClassicPiperGraphCommand, PiperGraphCommandBase, title="draw"):
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
            self.token, **({"port": self.port} if self.port else {})  # type: ignore
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


class DagBaseCommand(RunCommandBase):
    """Base class for Python DAG Object.
    Derived class should add custom fields (beware to not overwrite
    base class names and aliases!) and implement the `create_graph` method.
    The `input_mapping` and `output_mapping` properties can be used to return
    a custom name for the input and output data keys.
    """

    folder_debug: Path = Field(None, description="Path to Debug dir folder.")
    draw: t.Union[bool, Path, t.Mapping] = Field(
        False,
        description=(
            "Draw the graph and exit. `+draw` to just show, "
            "`+draw graph.png` to save to disk or "
            "`+draw.o graph.png +draw.b mermaid ...` for a full control on "
            "the underlying DrawCommand."
        ),
    )

    _nodes: T_NODES = PrivateAttr(None)

    @validator("folder_debug", always=True)
    def _validate_folder_debug(cls, v):
        from pipelime.choixe.utils.io import PipelimeTmp

        if not v:
            v = PipelimeTmp.make_subdir()
        v = v.resolve().absolute()
        logger.debug(f"DAG debug folder: {v}")
        return v

    @classmethod
    def init_from_checkpoint(cls, checkpoint: "CheckpointNamespace", /, **data):
        """Derived classes may override to support command resuming."""
        dbg_folder = checkpoint.read_data("folder_debug", None)
        if dbg_folder is not None:
            data["folder_debug"] = dbg_folder
            logger.debug(
                f"[{checkpoint.namespace}] Restoring debug folder "
                f"from checkpoint: {dbg_folder}"
            )
        return super().init_from_checkpoint(checkpoint, **data)

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

    def _auto_name_commands(
        self, nodes: t.Sequence[t.Union[PipelimeCommand, LazyCommand]]
    ) -> T_NODES:
        name_counter = {}
        node_map = {}
        for node in nodes:
            name = node.command_title()
            value = name_counter.setdefault(name, 0)
            name_counter[name] = value + 1
            node_map[name + f"-{value}"] = node
        return node_map

    def draw_graph(self, output: t.Optional[Path] = None, **kwargs) -> None:
        """Draws a pipelime DAG.

        Args:
            output (Path, optional): The output file. If not specified, the graph will
                be shown in a window. Defaults to None.
            **kwargs: Any other argument accepted by the DrawCommand.
        """
        nodes = self.nodes_graph
        if "o" not in kwargs and "output" not in kwargs:
            kwargs["output"] = output
        drawer = DrawCommand(
            nodes=nodes,
            include=self.include,
            exclude=self.exclude,
            skip_on_error=self.skip_on_error,
            start_from=self.start_from,
            stop_at=self.stop_at,
            **kwargs,
        )
        drawer.run()

    @abstractmethod
    def create_graph(self) -> t.Union[T_NODES, t.Sequence[PipelimeCommand]]:
        """Creates the graph nodes.

        Returns:
            t.Union[T_NODES, t.Sequence[PipelimeCommand]]: a dictionary containing the
                mapping between node names and nodes or a simple list of commands (names
                will be automatically generated)
        """

    @property
    def nodes_graph(self) -> T_NODES:
        if self._nodes is None:
            nodes = self.create_graph()
            if isinstance(nodes, t.Sequence):
                nodes = self._auto_name_commands(nodes)
            self._nodes = nodes
        return self._nodes

    def run(self) -> None:
        self.command_checkpoint.write_data("folder_debug", self.folder_debug.as_posix())

        self._validate_graph()
        if self.draw:
            output = self.draw if isinstance(self.draw, Path) else None
            self.draw_graph(
                output, **(self.draw if isinstance(self.draw, t.Mapping) else {})
            )
            return
        return super().run()


class PiperDAG(
    BaseModel,
    ABC,
    allow_population_by_field_name=True,
    extra="forbid",
    copy_on_model_validation="none",
):
    """Base class to ease the creation of Python DAG Object.

    Derived classes should add custom properties as pydantic fields and implement
    at least the `create_graph` method. Then, use the `piper_dag` decorator to create
    a full-fledged command class.

    Example:

        @piper_dag
        class MyGrandDAG(PiperDAG, title="mgd"):
            '''My Grand DAG'''

            aparam: str = Field(..., alias="a", description="a param")
            bparam: int = Field(12, alias="b", description="b param")

            def create_graph(self, folder_debug):
                ...
    """

    @property
    def input_mapping(self) -> t.Optional[t.Mapping[str, str]]:
        """Optional mapping from graph input data keys and desired keys."""
        return None

    @property
    def output_mapping(self) -> t.Optional[t.Mapping[str, str]]:
        """Optional mapping from graph output data keys and desired keys."""
        return None

    @abstractmethod
    def create_graph(
        self, folder_debug: Path
    ) -> t.Union[T_NODES, t.Sequence[PipelimeCommand]]:
        """Creates the graph nodes.

        Args:
            folder_debug (Path): The path to the folder for debug data.

        Returns:
            t.Union[T_NODES, t.Sequence[PipelimeCommand]]: a dictionary containing the
                mapping between node names and nodes or a simple list of commands (names
                will be automatically generated)
        """


def piper_dag(cls: t.Type[PiperDAG]):
    """A decorator to create a full-fledged command class from a PiperDAG class."""

    class _PiperDagCommandHelper(DagBaseCommand):
        class PropertyModel(cls):
            pass

        properties: PropertyModel = Field(
            ..., alias="p", description="DAG parameters", expand_help=True
        )

        @property
        def input_mapping(self) -> t.Optional[t.Mapping[str, str]]:
            """Optional mapping from graph input data keys and desired keys."""
            return self.properties.input_mapping

        @property
        def output_mapping(self) -> t.Optional[t.Mapping[str, str]]:
            """Optional mapping from graph output data keys and desired keys."""
            return self.properties.output_mapping

        def create_graph(self) -> t.Union[T_NODES, t.Sequence[PipelimeCommand]]:
            return self.properties.create_graph(self.folder_debug)

    dag_command = create_model(
        cls.__name__,
        __base__=_PiperDagCommandHelper,
        __module__=cls.__module__,
        __cls_kwargs__={"title": cls.schema()["title"]},
    )
    dag_command.__doc__ = cls.__doc__

    # give the inner class a more meaningful name
    dag_command.PropertyModel.__qualname__ = (
        f"{cls.__qualname__}.{dag_command.PropertyModel.__name__}"
    )
    dag_command.PropertyModel.__module__ = cls.__module__

    return dag_command
