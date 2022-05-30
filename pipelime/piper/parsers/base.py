from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Optional

from pipelime.piper.model import DAGModel


class DAGParser(ABC):
    @classmethod
    @abstractmethod
    def parse_cfg(cls, cfg: Dict, params: Optional[Dict] = None) -> DAGModel:
        """Parses the given configuration into a DAGModel.

        Args:
            cfg (Dict): The input configuration as dictionary
            params (Optional[Dict], optional): The parameters dictionary. Defaults to
            None.

        Returns:
            DAGModel: The parsed DAGModel
        """
        pass

    @classmethod
    @abstractmethod
    def parse_file(
        cls,
        cfg_file: Path,
        params_file: Optional[Path] = None,
        additional_args: Optional[Dict] = None,
    ) -> DAGModel:
        """Parse the given configuration file into a DAGModel.

        Args:
            cfg_file (str): The input configuration file
            params_file (Optional[str], optional): The parameters file. Defaults to
            None.
            additional_args (Optional[Dict], optional): Additional custom parameters
            that will overwrite the ones specified in params_file. Defaults to None.

        Returns:
            DAGModel: The parsed DAGModel
        """
        pass
