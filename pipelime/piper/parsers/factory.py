from typing import Sequence, Type

from pipelime.piper.parsers.base import DAGParser
from pipelime.piper.parsers.choixe_parser import ChoixeDAGParser


class DAGParserFactory:
    DEFAULT_PARSER = "ChoixeDAGParser"

    _parsers_map = {
        "ChoixeDAGParser": ChoixeDAGParser,
    }

    @classmethod
    def available_parsers(cls) -> Sequence[str]:
        return list(DAGParserFactory._parsers_map.keys())

    @classmethod
    def get_parser(cls, parser_name: str = DEFAULT_PARSER) -> Type[DAGParser]:
        return cls._parsers_map[parser_name]
