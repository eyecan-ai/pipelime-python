from abc import abstractmethod
from enum import Enum
import typing as t

from pydantic import BaseModel, Field, PrivateAttr

if t.TYPE_CHECKING:
    from pipelime.piper.progress.tracker.base import Tracker


# The return type is a hack to fool the type checker
# otherwise it would complain when calling the command
# TODO: forward with typing.ParamSpec (New in Python 3.10) to get full type checking
@t.overload
def pipelime_command(
    **__config_kwargs,
) -> t.Callable[[t.Callable[..., None]], t.Callable[..., t.Type["PipelimeCommand"]]]:
    ...


# The return type is a hack to fool the type checker
# otherwise it would complain when calling the command
# TODO: forward with typing.ParamSpec (New in Python 3.10) to get full type checking
@t.overload
def pipelime_command(
    __func: t.Callable[..., None]
) -> t.Callable[..., t.Type["PipelimeCommand"]]:
    ...


def pipelime_command(__func=None, **__config_kwargs):
    """Creates a full-fledged PipelimeCommand from a general function.
    The command will have the same exact signature and docstring of the function.
    Field names and types will taken from the function parameters and validated as usual.
    Any function signature is allowed, including positional-only, keyword-only,
    variable positional, variable keyword parameters. Variable positional and keyword
    can be annotated to get type checking and validation.

    You are encouraged to use pydantic.Field as default value to further specify any
    field properties, such as `default_factory`, `alias`, `description`, etc, which
    are used to show meaningful help messages through `pipelime help`.
    Also, call it with extra `__config_kwargs` to add any pydantic config parameters,
    such as `title`, `arbitrary_types_allowed`, etc.

    Examples:
        Create a command with the same name of the function::

            @pipelime_command
            def addup(a: int, b: int):
                print(a + b)

        then you can use it as::

            $ pipelime addup +a 1 +b 2

        Create a command with custom name and non-pydantic types::

            @pipelime_command(title="add-up-these", arbitrary_types_allowed=True)
            def my_addup_func(a: int, b: int, c: t.Optional[MyType] = None):
                print(a + b)

        then you can use it as::

            $ pipelime add-up-these +a 1 +b 2

        Create a command with field aliases and default factories::

            @pipelime_command
            def merge_lists(
                first: t.List[int] = Field(default_factory=list, alias="f"),
                second: t.List[int] = Field(default_factory=list, alias="s"),
            ):
                print(first + second)

        then you can use it as::

            $ pipelime merge_lists +f 1 +f 2 +f 3 +s 4 +s 5 +s 6

        Create a command with help messages::

            @pipelime_command(title="merge")
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
    """

    def _make_cmd(func):
        import inspect
        from pydantic.fields import FieldInfo, Undefined

        def _make_field(p: inspect.Parameter):
            """Returns a tuple of (annotation, default) for a given parameter.
            NB: *args translates to a Sequence and **kwargs translates to a Mapping.
            """
            value = Field(...) if p.default is inspect.Parameter.empty else p.default

            if p.kind is p.VAR_POSITIONAL:
                ann = (
                    t.Sequence
                    if p.annotation is inspect.Signature.empty
                    else t.Sequence[p.annotation]
                )
            elif p.kind is p.VAR_KEYWORD:
                ann = (
                    t.Mapping
                    if p.annotation is inspect.Signature.empty
                    else t.Mapping[str, p.annotation]
                )
            else:
                ann = p.annotation

            return (ann, value)

        # Translates signature to pydantic fields
        # and gathers positional arguments
        fsig = inspect.signature(func)
        fields = {n: _make_field(p) for n, p in fsig.parameters.items()}
        posonly_names = [
            p.name for p in fsig.parameters.values() if p.kind is p.POSITIONAL_ONLY
        ]
        poskw_names = [
            p.name
            for p in fsig.parameters.values()
            if p.kind is p.POSITIONAL_OR_KEYWORD
        ]
        kwonly_names = [
            p.name for p in fsig.parameters.values() if p.kind is p.KEYWORD_ONLY
        ]
        try:
            varpos_name = next(
                p.name for p in fsig.parameters.values() if p.kind is p.VAR_POSITIONAL
            )
        except StopIteration:
            varpos_name = ""
        try:
            varkw_name = next(
                p.name for p in fsig.parameters.values() if p.kind is p.VAR_KEYWORD
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
                func(
                    *[getattr(self, n) for n in posonly_names + poskw_names],
                    *getattr(self, varpos_name, tuple()),
                    **self.dict(include=set(kwonly_names + [varkw_name])),
                )

        # override base docstring with a custom description
        _FnModel.__doc__ = (
            f"Autogenerated Pipelime command from `{func.__module__}.{func.__name__}`."
        )

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
                if value in (Ellipsis, Undefined):
                    return inspect.Parameter.empty
            return value

        fmodel.__signature__ = fsig.replace(
            parameters=[
                p.replace(default=_unwrap_default(p.default))
                for p in fsig.parameters.values()
            ],
            return_annotation=inspect.Signature.empty,
        )
        return fmodel

    if __func is None:
        return _make_cmd
    return _make_cmd(__func)


class PiperPortType(Enum):
    INPUT = "input"
    OUTPUT = "output"
    PARAMETER = "parameter"


class PiperInfo(BaseModel, extra="forbid", copy_on_model_validation="none"):
    token: str = Field("", description="The piper execution token.")
    node: str = Field("", description="The piper dag's node name.")

    @property
    def active(self) -> bool:
        return bool(self.token)


class PipelimeCommand(
    BaseModel,
    allow_population_by_field_name=True,
    extra="forbid",
    copy_on_model_validation="none",
):
    """Base class for all pipelime commands.
    Subclasses should implement the run method.
    """

    _piper: PiperInfo = PrivateAttr(default_factory=PiperInfo)  # type: ignore
    _tracker: t.Optional["Tracker"] = PrivateAttr(None)

    @abstractmethod
    def run(self) -> None:
        pass

    @classmethod
    def _filter_fields_by_flag(cls, flag: str, value: t.Any) -> t.Iterable[str]:
        for k, v in cls.__fields__.items():
            if v.field_info.extra.get(flag, object()) == value:
                yield k

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

            cb = TrackCallbackFactory.get_callback()
            self._tracker = Tracker(self._piper.token, self._piper.node, cb)

        return self._tracker

    @classmethod
    def command_title(cls) -> str:
        if cls.__config__.title:
            return cls.__config__.title
        return cls.__name__

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
        import rich.progress

        if self._piper.active:
            tracker = self._get_piper_tracker()
            return tracker.track(seq, size=size, message=message)
        else:
            return rich.progress.track(
                seq,
                total=len(seq) if size is None else size,  # type: ignore
                description="🍋 " + message,
            )

    def __call__(self) -> None:
        self.run()


class NodesDefinition(BaseModel, extra="forbid", copy_on_model_validation="none"):
    """A simple interface to parse a DAG node configuration."""

    __root__: t.Mapping[str, PipelimeCommand]

    @classmethod
    def create(
        cls,
        value: t.Union[
            "NodesDefinition",
            t.Mapping[
                str,
                t.Union[
                    t.Mapping[str, t.Optional[t.Mapping[str, t.Any]]], "PipelimeCommand"
                ],
            ],
        ],
    ):
        return cls.validate(value)

    @property
    def value(self):
        return self.__root__

    def _iter(self, *args, **kwargs):
        for k, v in super()._iter(*args, **kwargs):
            # NB: `v` is the dict of params of the actual pipelime commands
            assert k == "__root__"
            assert isinstance(v, t.Mapping)
            yield k, {
                node_name: {self.__root__[node_name].command_title(): cmd_args}
                for node_name, cmd_args in v.items()
            }

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(
        cls,
        value: t.Union[
            "NodesDefinition",
            t.Mapping[
                str,
                t.Union[
                    t.Mapping[str, t.Optional[t.Mapping[str, t.Any]]], "PipelimeCommand"
                ],
            ],
        ],
    ):
        from pipelime.cli.utils import get_pipelime_command

        if isinstance(value, NodesDefinition):
            return value
        return cls(
            __root__={name: get_pipelime_command(cmd) for name, cmd in value.items()}
        )


class DAGModel(BaseModel, extra="forbid", copy_on_model_validation="none"):
    """A Piper DAG as a `<node>: <command>` mapping."""

    nodes: NodesDefinition

    def purged_dict(self):
        from pipelime.choixe import XConfig

        return XConfig(data={"nodes": self.nodes.value}).decode()
