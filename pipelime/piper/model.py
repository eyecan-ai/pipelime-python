from abc import abstractmethod
from enum import Enum
from typing import Any, Dict, Iterable, Optional, Sequence, Union

import rich.progress
from pydantic import BaseModel, Field, PrivateAttr

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
    _piper: PiperInfo = PrivateAttr(default_factory=PiperInfo)  # type: ignore
    _tracker: Optional[Tracker] = PrivateAttr(None)

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
            self._tracker = Tracker(self._piper.token, self._piper.node, cb)

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
        if self._piper.active:
            tracker = self._get_piper_tracker()
            return tracker.track(seq, size=size, message=message)
        else:
            return rich.progress.track(
                seq, total=len(seq) if size is None else size, description=message
            )

    def __call__(self) -> None:
        self.run()


class DAGModel(BaseModel):
    nodes: Dict[str, PipelimeCommand]

    def purged_dict(self):
        return XConfig(data={"nodes": self.nodes}).to_dict()
