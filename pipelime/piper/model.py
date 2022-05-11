from abc import abstractmethod
from enum import Enum
from typing import Any, Dict, Iterable, Optional, Sequence, Union

import rich.progress
from pydantic import BaseModel, Field

from pipelime.choixe import XConfig
from pipelime.piper.progress.tracker import TrackCallbackFactory, Tracker


class PiperPortType(Enum):
    INPUT = "input"
    OUTPUT = "output"
    PARAMETER = "parameter"


class PiperInfo(BaseModel):
    token: str = Field("", description="The piper execution token.")
    node: str = Field("", description="The piper dag's node name.")

    @property
    def active(self) -> bool:
        return len(self.token) > 0


class PipelimeCommand(BaseModel):
    piper: PiperInfo = Field(PiperInfo(), description="Piper details")  # type: ignore
    _tracker: Optional[Tracker] = None

    class Config:
        underscore_attrs_are_private = True

    @abstractmethod
    def run(self) -> None:
        pass

    @classmethod
    def _filter_fields_by_flag(cls, flag: str, value: Any) -> Iterable[str]:
        for k, v in cls.__fields__.items():
            if v.field_info.extra.get(flag, object()) == value:
                yield k

    def _get_fields_by_flag(self, flag: str, value: Any) -> Dict[str, Any]:
        return {k: getattr(self, k) for k in self._filter_fields_by_flag(flag, value)}

    def get_inputs(self) -> Dict[str, Any]:
        return self._get_fields_by_flag("piper_port", PiperPortType.INPUT)

    def get_outputs(self) -> Dict[str, Any]:
        return self._get_fields_by_flag("piper_port", PiperPortType.OUTPUT)

    def _get_piper_tracker(self) -> Tracker:
        if self._tracker is None:
            cb = TrackCallbackFactory.get_callback()
            self._tracker = Tracker(self.piper.token, self.piper.node, cb)

        return self._tracker

    @classmethod
    def command_title(cls) -> str:
        if cls.__config__.title:
            return cls.__config__.title
        return cls.__name__

    def command_name(self) -> str:
        return self.command_title()

    def track(
        self,
        seq: Union[Sequence, Iterable],
        *,
        size: Optional[int] = None,
        message: str = "",
    ) -> Iterable:
        if self.piper.active:
            tracker = self._get_piper_tracker()
            return tracker.track(seq, size=size, message=message)
        else:
            return rich.progress.track(
                seq, total=len(seq) if size is None else size, description=message
            )

    @classmethod
    def pretty_schema(
        cls, *, show_name: bool = True, indent: int = 0, indent_offs: int = 2
    ) -> str:
        import inspect

        schema_str = f"'{inspect.getdoc(cls)}' " + "{\n"
        if show_name:
            schema_str = ((" " * indent) + f"{cls.command_title()}: ") + schema_str

        for field in cls.__fields__.values():
            fname = field.name if not field.alias else field.alias

            if isinstance(field.type_, PipelimeCommand):
                fvalue = field.type_.pretty_schema(
                    show_name=False,
                    indent=indent_offs + indent,
                    indent_offs=indent_offs,
                )
            else:
                fhelp = (
                    f"'{field.field_info.description}' "
                    if field.field_info.description
                    else ""
                )
                fvalue = (
                    f"`{field.type_.__name__}`  "
                    + fhelp
                    + ("[required" if field.required else " [optional")
                    + f", default={field.get_default()}]"
                )
            schema_str += (" " * (indent_offs + indent)) + f"{fname}: {fvalue}\n"
        schema_str += (" " * indent) + "}"
        return schema_str

    def __call__(self) -> None:
        self.run()


class DAGModel(BaseModel):
    nodes: Dict[str, PipelimeCommand]

    def purged_dict(self):
        return XConfig(data={"nodes": self.nodes}).to_dict()
