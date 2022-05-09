from pathlib import Path
from typing import Dict, Optional

from pipelime.choixe import XConfig
from pipelime.piper.model import DAGModel
from pipelime.piper.parsers.base import DAGParser


class ChoixeDAGParser(DAGParser):
    @classmethod
    def parse_cfg(cls, cfg: Dict, params: Optional[Dict] = None) -> DAGModel:
        cfg = XConfig(cfg)
        context = XConfig(params)

        cfg = cfg.process(context)
        assert cfg.inspect().processed, "The configuration is still not processed"

        return DAGModel.parse_obj(cfg)

    @classmethod
    def parse_file(
        cls,
        cfg_file: Path,
        params_file: Optional[Path] = None,
        additional_args: Optional[Dict] = None,
    ) -> DAGModel:
        cfg = XConfig.from_file(cfg_file)
        params = XConfig() if params_file is None else XConfig.from_file(params_file)
        add_args = XConfig() if additional_args is None else XConfig(additional_args)
        params.deep_update(add_args)
        return cls.parse_cfg(cfg.to_dict(), params.to_dict())
