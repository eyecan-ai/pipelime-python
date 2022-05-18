from pipelime.piper.parsers.factory import DAGParserFactory
from pipelime.piper.parsers.base import DAGParser


class TestDAGParserFactory:
    def test_available_parsers(self):
        # Get available parsers
        available = DAGParserFactory.available_parsers()

        # Check if the list contains at least one parser
        assert isinstance(available, list)
        assert len(available) > 0

        # Check that the available parsers are all strings
        for x in available:
            assert isinstance(x, str)

    def test_get_parser(self):
        # Get a parser
        parser = DAGParserFactory.get_parser()

        # Check that the parser is a DAGParser
        assert isinstance(parser, DAGParser)
