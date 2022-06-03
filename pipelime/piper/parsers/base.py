from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Optional

from pipelime.piper.model import DAGModel


class DAGParser(ABC):
    @abstractmethod
    def parse_cfg(self, cfg: Dict, params: Optional[Dict] = None) -> DAGModel:
        """Parses the given configuration into a DAGModel.

        Args:
            cfg (Dict): The input configuration as dictionary
            params (Optional[Dict], optional): The parameters dictionary. Defaults to
            None.

        Returns:
            DAGModel: The parsed DAGModel
        """
        pass

    @abstractmethod
    def _read_file(self, path: Path, additional_args: Optional[Dict] = None) -> Dict:
        """Reads the given file and returns its content as dictionary.

        Args:
            path (Path): The path to the file
            additional_args (Optional[Dict], optional): Additional parameters that will
            overwrite the ones specified in the file. Defaults to None.

        Returns:
            Dict: The file content as dictionary
        """
        pass

    def parse_file(
        self,
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
        cfg = self._read_file(cfg_file)
        if params_file is not None:
            params = self._read_file(params_file, additional_args=additional_args)
        elif additional_args is not None:
            params = additional_args
        else:
            params = {}
        return self.parse_cfg(cfg, params)
