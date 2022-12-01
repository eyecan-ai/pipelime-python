from pathlib import Path
from typing import Mapping, Optional

from pipelime.choixe import XConfig
from pipelime.piper.model import DAGModel
from pipelime.piper.parsers.base import DAGParser


class ChoixeDAGParser(DAGParser):
    def parse_cfg(self, cfg: Mapping, params: Optional[Mapping] = None) -> DAGModel:
        cfg = XConfig(cfg)
        context = XConfig(params)

        cfg = cfg.process(context)
        assert cfg.inspect().processed, "The configuration is still not processed"

        return DAGModel.parse_obj(cfg.to_dict())

    def _read_file(
        self, path: Path, additional_args: Optional[Mapping] = None
    ) -> Mapping:
        data = XConfig.from_file(path)
        additional_args = {} if additional_args is None else additional_args
        data.deep_update(additional_args)  # type: ignore
        return data
