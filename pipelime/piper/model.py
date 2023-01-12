from abc import abstractmethod
from enum import Enum
import typing as t

from pydantic import BaseModel, Field, PrivateAttr

from pipelime.piper.progress.tracker.base import Tracker


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
    _tracker: t.Optional[Tracker] = PrivateAttr(None)

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

    def _get_piper_tracker(self) -> Tracker:
        if self._tracker is None:  # pragma: no branch
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
