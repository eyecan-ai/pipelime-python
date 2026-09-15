import typing as t
from abc import ABC, abstractmethod
from enum import Enum

import typing_extensions as te
from loguru import logger
from pydantic import PrivateAttr, model_serializer

from pipelime.piper.checkpoint import CheckpointNamespace
from pipelime.piper.progress.tracker.base import TrackCallback, TrackedTask, Tracker
from pipelime.utils.pydantic_compat import (
    Field,
    PipelimeModel,
    PipelimeRootModel,
    iter_fields,
    model_title,
)


# The return type is a hack to fool the type checker
# otherwise it would complain when calling the command
# TODO: forward with typing.ParamSpec (New in Python 3.10) to get full type checking
@t.overload
def command(
    *,
    title: t.Optional[str] = None,
    **__config_kwargs,
) -> t.Callable[
    [t.Callable[..., None]], t.Callable[..., t.Type["PipelimeCommand"]]
]: ...


# The return type is a hack to fool the type checker
# otherwise it would complain when calling the command
# TODO: forward with typing.ParamSpec (New in Python 3.10) to get full type checking
@t.overload
def command(
    __func: t.Callable[..., None]
) -> t.Callable[..., t.Type["PipelimeCommand"]]: ...


def command(__func=None, *, title: t.Optional[str] = None, **__config_kwargs):
    """Creates a full-fledged PipelimeCommand from a general function.
    The command will have the same exact signature and docstring of the function.
    Field names and types will taken from the function parameters and validated as
    usual. Any function signature is allowed, including positional-only, keyword-only,
    variable positional, variable keyword parameters. Variable positional and keyword
    can be annotated to get type checking and validation.

    You are encouraged to use pydantic.Field as default value to further specify any
    field properties, such as `default_factory`, `alias`, `description`, etc, which
    are used to show meaningful help messages through `pipelime help`.
    Also, call it with extra `__config_kwargs` to add any pydantic config parameters,
    such as `title`, `arbitrary_types_allowed`, etc.

    NB: if the first function argument is named `self`, the command will be bound to it.

    Args:
        __func: function to be converted to a PipelimeCommand.
        title: the title of the command.
        __config_kwargs: extra pydantic config parameters.

    Returns:
        A PipelimeCommand class.

    Examples:
        Create a command with the same name of the function::

            @command
            def addup(a: int, b: int):
                print(a + b)

        then you can use it as::

            $ pipelime addup +a 1 +b 2

        Create a command with custom name and non-pydantic types::

            @command(title="add-up-these", arbitrary_types_allowed=True)
            def my_addup_func(a: int, b: int, c: t.Optional[MyType] = None):
                print(a + b)

        then you can use it as::

            $ pipelime add-up-these +a 1 +b 2

        Create a command with field aliases and default factories::

            @command
            def merge_lists(
                first: t.List[int] = Field(default_factory=list, alias="f"),
                second: t.List[int] = Field(default_factory=list, alias="s"),
            ):
                print(first + second)

        then you can use it as::

            $ pipelime merge_lists +f 1 +f 2 +f 3 +s 4 +s 5 +s 6

        Create a command with help messages::

            @command(title="merge")
            def merge_lists(
                first: t.List[int] = Field(
                    default_factory=list, alias="f", description="first list"
                ),
                second: t.List[int] = Field(
                    default_factory=list, alias="s", description="second list"
                ),
            ):
                '''Merge and print two lists.'''
                print(first + second)

        then show the command help::

            $ pipelime merge help

        Create a command which starts a pipelime task::

            @command(bind=True)
            def copy_data(
                self,
                src: InputDatasetInterface,
                dst: OutputDatasetInterface,
                grabber: GrabberInterface,
            ):
                seq = src.create_reader()
                seq = dst.append_writer(seq)
                grabber.grab_all(
                    seq,
                    grab_context_manager=dst.serialization_cm(),
                    keep_order=False,
                    parent_cmd=self,
                    track_message=f"Copying...",
                )
    """

    if title is not None:
        __config_kwargs["title"] = title

    def _make_cmd(func):
        import inspect

        from pydantic.fields import FieldInfo
        from pydantic_core import PydanticUndefined

        def _make_field(p: inspect.Parameter):
            """Returns a tuple of (annotation, default) for a given parameter.
            NB: *args translates to a tuple and **kwargs translates to a dict.
            """
            value = Field(...) if p.default is inspect.Parameter.empty else p.default
            no_ann = p.annotation is inspect.Signature.empty

            if p.kind is p.VAR_POSITIONAL:
                ann = tuple if no_ann else tuple[p.annotation, ...]
            elif p.kind is p.VAR_KEYWORD:
                ann = dict if no_ann else dict[str, p.annotation]
            elif no_ann:
                # pydantic v2 needs an annotation: infer it from the default as v1 did
                raw = value
                if isinstance(value, FieldInfo):
                    raw = (
                        value.default_factory()
                        if value.default_factory is not None
                        else value.default
                    )
                ann = (
                    t.Any
                    if raw is None or raw is PydanticUndefined or raw is Ellipsis
                    else type(raw)
                )
            else:
                ann = p.annotation

            return (ann, value)

        # Translates signature to pydantic fields
        # and gathers positional arguments
        fsig = inspect.signature(func)
        fsig_params = fsig.parameters
        is_bound = (
            fsig.parameters and next(iter(fsig.parameters.values())).name == "self"
        )
        if is_bound:
            fsig_params = {k: v for k, v in fsig_params.items() if k != "self"}

        fields = {n: _make_field(p) for n, p in fsig_params.items()}
        posonly_names = [
            p.name for p in fsig_params.values() if p.kind is p.POSITIONAL_ONLY
        ]
        poskw_names = [
            p.name for p in fsig_params.values() if p.kind is p.POSITIONAL_OR_KEYWORD
        ]
        kwonly_names = [
            p.name for p in fsig_params.values() if p.kind is p.KEYWORD_ONLY
        ]
        try:
            varpos_name = next(
                p.name for p in fsig_params.values() if p.kind is p.VAR_POSITIONAL
            )
        except StopIteration:
            varpos_name = ""
        try:
            varkw_name = next(
                p.name for p in fsig_params.values() if p.kind is p.VAR_KEYWORD
            )
        except StopIteration:
            varkw_name = ""

        # set title to function name, if not specified
        __config_kwargs.setdefault("title", func.__name__)

        class _FnModel(PipelimeCommand):
            def __init__(self, *args, **kwargs):
                # positional-only arguments are not allowed in kwargs
                for p in posonly_names + [varpos_name]:
                    if p in kwargs:
                        raise TypeError(
                            f"Argument '{p}' in {self.command_name} is positional-only"
                        )

                # positional arguments must be less than declared ones
                if not varpos_name and len(args) > len(posonly_names + poskw_names):
                    raise TypeError(
                        f"{self.command_name} takes {len(posonly_names + poskw_names)} "
                        f"positional arguments but {len(args)} were given"
                    )

                # move positional arguments to data
                data = {}
                for name, value in zip(posonly_names, args):
                    data[name] = value
                for name, value in zip(poskw_names, args[len(posonly_names) :]):
                    if name in kwargs:
                        raise TypeError(
                            f"{self.command_name} got multiple values "
                            f"for argument '{name}'"
                        )
                    data[name] = value
                if varpos_name:
                    data[varpos_name] = tuple(
                        args[len(posonly_names) + len(poskw_names) :]
                    )

                # move keyword arguments to data
                for name in poskw_names + kwonly_names:
                    if name in kwargs:
                        data[name] = kwargs.pop(name)
                if varkw_name:
                    data[varkw_name] = kwargs
                    kwargs = {}

                # kwargs should be passed (user may have set extra="allow")
                super(_FnModel, self).__init__(**data, **kwargs)

            def run(self):
                # get all arguments in the right order
                # NB: do not use self.model_dump(), it returns sub-models as dict!
                self_arg = (self,) if is_bound else tuple()
                pos_args = [getattr(self, n) for n in posonly_names + poskw_names]
                var_args = getattr(self, varpos_name) if varpos_name else tuple()
                kw_args = {n: getattr(self, n) for n in kwonly_names if hasattr(self, n)}
                if varkw_name and hasattr(self, varkw_name):
                    kw_args.update(getattr(self, varkw_name))  # expand **kwargs (bug fix)
                func(*self_arg, *pos_args, *var_args, **kw_args)

        # override base docstring with a custom description
        _FnModel.__doc__ = f"Autogenerated Pipelime command from `{func.__module__}.{func.__qualname__}`."

        # create the pipelime command class
        fmodel = type(
            func.__name__.replace("_", " ").title().replace(" ", "") + "Command",
            (_FnModel,),
            {
                "__module__": func.__module__,
                "__doc__": func.__doc__,
                "__annotations__": {
                    n: f[0]
                    for n, f in fields.items()
                    if f[0] is not inspect.Signature.empty
                },
                **{n: f[1] for n, f in fields.items()},
            },
            **__config_kwargs,
        )

        # override pydantic signature
        # --> we need this to pretty print the command help
        def _unwrap_default(value):
            if isinstance(value, FieldInfo):
                value = (
                    value.default
                    if value.default_factory is None
                    else value.default_factory()
                )
                if value is Ellipsis or value is PydanticUndefined:
                    return inspect.Parameter.empty
            return value

        try:
            fmodel.__signature__ = fsig.replace(
                parameters=[
                    p.replace(default=_unwrap_default(p.default))
                    for p in fsig_params.values()
                ],
                return_annotation=inspect.Signature.empty,
            )
        except ValueError as e:
            raise TypeError(
                "Invalid command signature for @command "
                f"`{func.__module__}.{func.__qualname__}`: {str(e)}"
            ) from e
        return fmodel

    if __func is None:
        return _make_cmd
    return _make_cmd(__func)


def self_() -> "PipelimeCommand":
    from pipelime.utils.inspection import MyCaller

    parent = MyCaller(1).locals.get("self", None)
    if parent is None or not isinstance(parent, PipelimeCommand):
        raise TypeError(
            "self_() can only be used inside a function decorated as @command."
        )
    return parent


class PiperPortType(Enum):
    INPUT = "input"
    OUTPUT = "output"
    PARAMETER = "parameter"


class PiperInfo(PipelimeModel, extra="forbid"):
    token: str = Field("", description="The piper execution token.")
    node: str = Field("", description="The piper dag's node name.")

    @property
    def active(self) -> bool:
        return bool(self.token)


class PipelimeCommand(PipelimeModel, ABC, populate_by_name=True, extra="forbid"):
    """Base class for all pipelime commands. Subclasses should implement the run method.

    Extra class definition kwargs:
        force_gc (bool): force garbage collection after the command is executed.
        no_default_checkpoint (bool): do not save the command in the default checkpoint.
    """

    _force_gc: t.ClassVar[bool] = False
    _no_default_checkpoint: t.ClassVar[bool] = False
    _track_callback: t.ClassVar[t.Optional[TrackCallback]] = None

    _piper: PiperInfo = PrivateAttr(default_factory=PiperInfo)  # type: ignore
    _tracker: t.Optional["Tracker"] = PrivateAttr(None)
    _checkpoint: CheckpointNamespace = PrivateAttr(default_factory=CheckpointNamespace)

    @classmethod
    def lazy(cls) -> t.Type[te.Self]:
        outer_cls = cls

        class _LazyCommand(outer_cls):
            def __new__(cls, *args, **kwargs) -> LazyCommand[outer_cls]:
                lc = LazyCommand[outer_cls].__new__(LazyCommand)
                lc.__init__(command_class=outer_cls, data=kwargs)  # type: ignore
                return lc

        return _LazyCommand

    @classmethod
    def __init_subclass__(
        cls, force_gc: bool = False, no_default_checkpoint: bool = False, **kwargs
    ):
        cls._force_gc = force_gc
        cls._no_default_checkpoint = no_default_checkpoint

    @classmethod
    def save_to_default_checkpoint(cls) -> bool:
        return not cls._no_default_checkpoint

    @classmethod
    def init_from_checkpoint(cls, checkpoint: CheckpointNamespace, /, **data):
        """Derived classes may override to support command resuming."""
        return cls(**data)

    @property
    def command_checkpoint(self) -> CheckpointNamespace:
        return self._checkpoint

    @abstractmethod
    def run(self) -> None:
        pass

    @classmethod
    def _filter_fields_by_flag(cls, flag: str, value: t.Any) -> t.Iterable[str]:
        for fview in iter_fields(cls):
            if fview.extra.get(flag, object()) == value:
                yield fview.name

    def _get_fields_by_flag(self, flag: str, value: t.Any) -> t.Dict[str, t.Any]:
        return {k: getattr(self, k) for k in self._filter_fields_by_flag(flag, value)}

    def get_inputs(self) -> t.Dict[str, t.Any]:
        return self._get_fields_by_flag("piper_port", PiperPortType.INPUT)

    def get_outputs(self) -> t.Dict[str, t.Any]:
        return self._get_fields_by_flag("piper_port", PiperPortType.OUTPUT)

    def _get_piper_tracker(self) -> "Tracker":
        if self._tracker is None:  # pragma: no branch
            from pipelime.piper.progress.tracker.base import Tracker
            from pipelime.piper.progress.tracker.factory import TrackCallbackFactory

            cb = self._track_callback or TrackCallbackFactory.get_callback()
            self._tracker = Tracker(self._piper.token, self._piper.node, cb)

        return self._tracker

    @classmethod
    def command_title(cls) -> str:
        return model_title(cls)

    @property
    def command_name(self) -> str:
        return self.command_title()

    def set_piper_info(
        self,
        *,
        token: t.Optional[str] = None,
        node: t.Optional[str] = None,
    ):
        if token is not None:
            self._piper.token = token
        if node is not None:
            self._piper.node = node

    def track(
        self,
        seq: t.Union[t.Sequence, t.Iterable],
        *,
        size: t.Optional[int] = None,
        message: str = "",
    ) -> t.Iterable:
        """Track a sequence with a progress bar or a piper task."""

        total = len(seq) if size is None else size  # type: ignore
        with self.create_task(total, message) as t:
            yield from t.track(seq)

    def create_task(self, total: int, message: str = "") -> "TrackedTask":
        """Explicit piper task creation."""
        from pipelime.piper.progress.tracker.base import TqdmTask

        if self._piper.active:
            return self._get_piper_tracker().create_task(total, message)
        return TqdmTask(total, message)

    def __call__(self) -> None:
        self.run()

        if self._force_gc:
            import gc

            gc.collect()


CmdTp = t.TypeVar("CmdTp", bound=PipelimeCommand)


class LazyCommand(PipelimeModel, t.Generic[CmdTp], extra="forbid"):
    command_class: t.Type[CmdTp]
    data: t.Dict[str, t.Any] = Field(default_factory=dict)

    def __call__(self) -> CmdTp:
        return self.command_class(**self.data)

    def init_from_checkpoint(self, checkpoint: CheckpointNamespace):
        return self.command_class.init_from_checkpoint(checkpoint, **self.data)

    def __setattr__(self, name, value):
        self.data[name] = value

    def __getattr__(self, name):
        if name.startswith("__") or name == "data":  # never recurse into ourselves
            raise AttributeError(name)
        if name in self.data:
            return self.data[name]
        if name in self.command_class.model_fields:
            return self.command_class.model_fields[name].get_default(
                call_default_factory=True
            )
        if hasattr(self.command_class, name):
            return getattr(self.command_class, name)
        raise AttributeError(f"{self.command_class.__name__} has no attribute '{name}'")


T_DAG_NODE = t.Union[
    PipelimeCommand, LazyCommand, t.Mapping[str, t.Optional[t.Mapping[str, t.Any]]]
]
T_NODES = t.Mapping[str, T_DAG_NODE]


class NodesDefinition(PipelimeRootModel[t.Mapping[str, PipelimeCommand]]):
    """A simple interface to parse a DAG node configuration."""

    @classmethod
    def _build_nodes(
        cls,
        value: T_NODES,
        *,
        checkpoint: t.Optional[CheckpointNamespace] = None,
        skip_on_error: bool = False,
    ) -> t.Dict[str, PipelimeCommand]:
        from pydantic import ValidationError

        from pipelime.cli.utils import format_validation_error, resolve_pipelime_command

        plnodes = {}
        for name, cmd in value.items():
            if checkpoint:
                ckpt = checkpoint.get_namespace(
                    name.replace(".", "_").replace("[", "_").replace("]", "_")
                )
            else:
                ckpt = None

            cmd_cls, build = resolve_pipelime_command(cmd, ckpt)
            try:
                plcmd = build()
            except ValidationError as e:
                if skip_on_error:
                    logger.warning(f"Skipping node `{name}` due to validation error.")
                else:
                    raise ValueError(
                        f"Invalid node definition `{name}`:\n"
                        f"{format_validation_error(e, cmd_cls)}"
                    ) from e
            else:
                plnodes[name] = plcmd
        return plnodes

    @classmethod
    def create(
        cls,
        value: t.Union["NodesDefinition", T_NODES],
        *,
        checkpoint: t.Optional[CheckpointNamespace] = None,
        skip_on_error: bool = False,
    ):
        if isinstance(value, NodesDefinition):
            return value
        return cls(cls._build_nodes(value, checkpoint=checkpoint, skip_on_error=skip_on_error))

    @classmethod
    def _coerce(cls, value):
        if isinstance(value, t.Mapping) and all(
            isinstance(v, PipelimeCommand) for v in value.values()
        ):
            return value
        return cls._build_nodes(value)

    @model_serializer(mode="wrap")
    def _serialize(self, handler) -> t.Dict[str, t.Any]:
        # NB: `handler` gives `{node_name: <command args>}` with the caller's flags applied
        return {
            node_name: {self.root[node_name].command_title(): cmd_args}
            for node_name, cmd_args in handler(self).items()
        }


class DAGModel(PipelimeModel, extra="forbid"):
    """A Piper DAG as a `<node>: <command>` mapping."""

    nodes: NodesDefinition

    def purged_dict(self):
        from pipelime.choixe import XConfig

        return XConfig(data={"nodes": self.nodes.value}).decode()
