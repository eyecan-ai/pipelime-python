import pytest
import typing as t
from pathlib import Path
from pydantic import BaseModel
import yaml
from pipelime.piper.parsers.factory import DAGParserFactory
from pipelime.piper.parsers.base import DAGParser
from pipelime.piper.model import DAGModel


class TestDAGParserFactory:
    def test_available_parsers(self):
        # Get available parsers
        available = DAGParserFactory.available_parsers()

        # Check if the list contains at least one parser
        assert isinstance(available, t.Sequence)
        assert len(available) > 0

        # Check that the available parsers are all strings
        for x in available:
            assert isinstance(x, str)

    def test_get_parser(self):
        for x in DAGParserFactory.available_parsers():
            # Get a parser
            parser = DAGParserFactory.get_parser(x)

            # Check that the parser is a DAGParser
            assert isinstance(parser, DAGParser)

    @pytest.mark.parametrize("parser", DAGParserFactory.available_parsers())
    def test_parse(self, parser, all_dags):
        parser = DAGParserFactory.get_parser(parser)
        for dag in all_dags:
            if dag["ctx_path"].exists():
                # direct model parsing
                dag_model_ref = DAGModel.parse_obj(dag["config"])
                dag_model_ref = self._purge_paths(dag_model_ref)

                # file parsing + choixe processing
                dag_file = parser.parse_file(dag["cfg_path"], dag["ctx_path"])
                dag_file = self._purge_paths(dag_file)
                assert dag_model_ref == dag_file

                # dict parsing + choixe processing
                with open(dag["cfg_path"], "r") as fcfg, open(
                    dag["ctx_path"], "r"
                ) as fctx:
                    cfg = yaml.safe_load(fcfg)
                    ctx = yaml.safe_load(fctx)
                    dag_cfg = parser.parse_cfg(cfg, ctx)
                    dag_cfg = self._purge_paths(dag_cfg)
                    assert dag_model_ref == dag_cfg

    @pytest.mark.parametrize("parser", DAGParserFactory.available_parsers())
    def test_parse_additional_parameters(self, parser, all_dags, tmp_path: Path):
        parser = DAGParserFactory.get_parser(parser)
        for dag in all_dags:
            if dag["ctx_path"].exists():
                # direct model parsing
                dag_model_ref = DAGModel.parse_obj(dag["config"])
                dag_model_ref = self._purge_paths(dag_model_ref)

                with open(dag["cfg_path"], "r") as fcfg, open(
                    dag["ctx_path"], "r"
                ) as fctx:
                    cfg = yaml.safe_load(fcfg)
                    ctx = yaml.safe_load(fctx)

                    # additional parameters
                    ref_k = next(iter(ctx))
                    additional_prms = {ref_k: self._hash_values(ctx[ref_k])}

                    # reference parsing
                    effective_ctx = {**ctx, **additional_prms}
                    assert effective_ctx != ctx

                    dag_parse_ref = parser.parse_cfg(cfg, effective_ctx)
                    dag_parse_ref = self._purge_paths(dag_parse_ref)
                    assert dag_parse_ref != dag_model_ref

                    # parse file with additional parameters
                    dag_file = parser.parse_file(
                        dag["cfg_path"],
                        dag["ctx_path"],
                        additional_args=additional_prms,
                    )
                    dag_file = self._purge_paths(dag_file)
                    assert dag_parse_ref == dag_file

                    # parse file with only additional parameters
                    dag_file = parser.parse_file(
                        dag["cfg_path"], additional_args=effective_ctx
                    )
                    dag_file = self._purge_paths(dag_file)
                    assert dag_parse_ref == dag_file

                    # parse file with no params
                    outfile = tmp_path / "noprms.yaml"
                    with outfile.open("w") as f:
                        yaml.safe_dump(dag_parse_ref, f)
                        dag_file = parser.parse_file(outfile)
                        dag_file = self._purge_paths(dag_file)
                        assert dag_parse_ref == dag_file

    def _purge_paths(self, value):
        if isinstance(value, t.Mapping):
            value = {k: self._purge_paths(v) for k, v in value.items()}
        elif isinstance(value, t.Sequence) and not isinstance(value, (str, bytes)):
            value = [self._purge_paths(x) for x in value]
        elif isinstance(value, BaseModel):
            value = self._purge_paths(value.dict(by_alias=True))
        elif isinstance(value, (str, Path)) and (
            "tmp" in str(value)
            or "Temp" in str(value)
            or "temp" in str(value)
            or "TEMP" in str(value)
        ):
            value = Path(value).name
        elif isinstance(value, Path):
            value = value.name
        return value

    def _hash_values(self, value):
        if isinstance(value, t.Mapping):
            value = {hash(k): self._hash_values(v) for k, v in value.items()}
        elif isinstance(value, t.Sequence) and not isinstance(value, (str, bytes)):
            value = [self._hash_values(x) for x in value]
        elif isinstance(value, BaseModel):
            value = self._hash_values(value.dict(by_alias=True))
        else:
            value = hash(str(value))
        return value
