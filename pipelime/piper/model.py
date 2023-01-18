from abc import abstractmethod
from enum import Enum
import typing as t

from pydantic import BaseModel, Field, PrivateAttr, create_model

if t.TYPE_CHECKING:
    from pipelime.piper.progress.tracker.base import Tracker


def pipelime_command(func=None, **kwargs):
    def _make_cmd(func):
        import inspect

        posonly_args, poskw_args, varpos_args = "", "", ""
        posonly_names, poskw_names, varpos_name = [], [], ""

        def _make_field(p: inspect.Parameter):
            nonlocal posonly_args, poskw_args, varpos_args
            nonlocal posonly_names, poskw_names, varpos_name

            value = ... if p.default is inspect.Parameter.empty else p.default

            ann = p.annotation
            if p.kind is p.VAR_POSITIONAL:
                varpos_args = f"{p}, ".replace("NoneType", "None")
                varpos_name = p.name
                ann = (
                    t.Sequence
                    if p.annotation is inspect.Signature.empty
                    else t.Sequence[p.annotation]
                )
            elif p.kind is p.POSITIONAL_ONLY:
                posonly_args += f"{p}, ".replace("NoneType", "None")
                posonly_names.append(p.name)
            elif p.kind is p.POSITIONAL_OR_KEYWORD:
                poskw_args += f"{p}, ".replace("NoneType", "None")
                poskw_names.append(p.name)

            if ann is inspect.Signature.empty:
                return (t.Any, value) if value is ... else value
            return (ann, value)

        fsig = inspect.signature(func)
        fields = {
            n: _make_field(p)
            for n, p in fsig.parameters.items()
            if p.kind is not p.VAR_KEYWORD
        }

        if any(p.kind is p.VAR_KEYWORD for p in fsig.parameters.values()):
            kwargs.setdefault("extra", "allow")
            add_data_arg = True
        else:
            add_data_arg = any(
                p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
                for p in fsig.parameters.values()
            )

        kwargs.setdefault("title", func.__name__)

        if posonly_args:
            posonly_args += "/, "
        pos_names = posonly_names + poskw_names

        class _FnModel(PipelimeCommand):
            def run(self):
                func(
                    *[getattr(self, n) for n in pos_names],
                    *getattr(self, varpos_name, tuple()),
                    **self.dict(exclude=set(pos_names + [varpos_name])),
                )

        local_scope = {**globals(), **locals()}
        _exfn = """
from typing import *
def initfn(self, {}{}{}{}
""".format(
            posonly_args,
            poskw_args,
            varpos_args,
            "**__data):\n" if add_data_arg else "):\n    __data = {}",
        )

        if varpos_name:
            _exfn += "    __data.setdefault('{0}', {0})\n".format(varpos_name)
        for p in pos_names:
            _exfn += "    __data.setdefault('{0}', {0})\n".format(p)
        _exfn += "    super(_FnModel, self).__init__(**__data)\n"

        exec(_exfn, local_scope)
        _FnModel.__init__ = local_scope["initfn"]

        fmodel = create_model(
            func.__name__.replace("_", " ").capitalize().strip() + "Command",
            __base__=_FnModel,
            __module__=func.__module__,
            __cls_kwargs__=kwargs,
            **fields,
        )
        fmodel.__doc__ = func.__doc__

        return fmodel

    if func is None:
        return _make_cmd
    return _make_cmd(func)


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
