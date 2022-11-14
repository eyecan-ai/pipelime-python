import pytest
from copy import deepcopy
from pathlib import Path

from deepdiff import DeepDiff
from schema import Or, Schema, Use

from pipelime.choixe.ast.parser import parse
from pipelime.choixe.utils.io import load
from pipelime.choixe.visitors import process, walk
from pipelime.choixe.visitors.inspector import Inspection
from pipelime.choixe.xconfig import XConfig


class TestXConfig:
    def _copy_test(self, cfg: XConfig):
        cfg_copy = cfg.copy()
        assert not DeepDiff(cfg_copy.decode(), cfg.decode())
        assert cfg_copy.get_schema() == cfg.get_schema()
        assert cfg_copy.get_cwd() == cfg.get_cwd()

    def test_from_file(self, choixe_plain_cfg: Path):
        cfg = XConfig.from_file(choixe_plain_cfg)
        assert not DeepDiff(cfg.decode(), load(choixe_plain_cfg))
        assert cfg.get_cwd() == choixe_plain_cfg.parent
        self._copy_test(cfg)

    def test_from_dict(self, choixe_plain_cfg: Path):
        data = load(choixe_plain_cfg)
        cfg = XConfig(data=data)
        assert not DeepDiff(cfg.decode(), data)
        assert cfg.get_cwd() is None
        self._copy_test(cfg)

    def test_from_nothing(self):
        cfg = XConfig()
        assert not DeepDiff(cfg.decode(), {})
        assert cfg.get_cwd() is None
        self._copy_test(cfg)

    def test_file_io(self, tmp_path: Path, choixe_plain_cfg: Path):
        cfg = XConfig.from_file(choixe_plain_cfg)
        save_path = tmp_path / "config.yml"
        cfg.save_to(save_path)
        re_cfg = XConfig.from_file(save_path)

        assert re_cfg.get_cwd() == save_path.parent
        assert not DeepDiff(re_cfg.decode(), cfg.decode())

    @pytest.mark.parametrize("replace", [True, False])
    def test_with_schema(self, choixe_plain_cfg: Path, replace: bool):
        cfg = XConfig.from_file(choixe_plain_cfg)
        assert cfg.is_valid()  # No schema: always valid

        schema = Schema(
            {"alice": int, "bob": int, "charlie": [Or(str, {str: Use(int)})]}
        )
        cfg.set_schema(schema)
        assert cfg.get_schema() == schema
        assert cfg.is_valid()
        cfg.validate(replace=replace)
        expected = schema.validate(load(choixe_plain_cfg))
        assert bool(DeepDiff(cfg.decode(), expected)) != replace

    @pytest.mark.parametrize("only_valid_keys", [True, False])
    @pytest.mark.parametrize("append_values", [True, False])
    def test_deep_keys(
        self, choixe_plain_cfg: Path, only_valid_keys: bool, append_values: bool
    ):
        cfg = XConfig.from_file(choixe_plain_cfg)
        assert cfg.deep_get("charlie[2].alpha") == 10.0
        assert cfg.deep_get(["charlie", 2, "beta"]) == 20.0
        assert cfg.deep_get("bob.alpha", default="hello") == "hello"

        cfg.deep_set(
            "charlie[2].alpha",
            40,
            only_valid_keys=only_valid_keys,
            append_values=append_values,
        )
        assert cfg.deep_get("charlie[2].alpha") == ([10.0, 40] if append_values else 40)

        cfg.deep_set(
            "charlie[3].foo.bar",
            [10, 20, 30],
            only_valid_keys=only_valid_keys,
            append_values=append_values,
        )
        v = cfg.deep_get("charlie[3]")
        if only_valid_keys:
            assert v is None
        else:
            assert v == {"foo": {"bar": [10, 20, 30]}}

        cfg.deep_set(
            "charlie[3].foo.bar",
            [40, 50],
            only_valid_keys=only_valid_keys,
            append_values=append_values,
        )
        v = cfg.deep_get("charlie[3].foo.bar")
        if only_valid_keys:
            assert v is None
        else:
            assert v == [10, 20, 30, 40, 50] if append_values else [40, 50]

        cfg.deep_set(
            "dino[2][3].saur",
            42,
            only_valid_keys=only_valid_keys,
            append_values=append_values,
        )
        if only_valid_keys:
            assert cfg.deep_get("dino") is None
        else:
            assert cfg.deep_get("dino[0]") is None
            assert cfg.deep_get("dino[2][0]") is None
            assert cfg.deep_get("dino[2][3].saur") == 42

    def test_deep_update(self):
        data = {
            "a": {"b": 10},
            "b": [0, 2, 1.02],
            "c": {"a": "b", "b": [{"a": 1, "b": 2}, "a"]},
        }
        other = {"c": {"b": [{"a": 2}], "e": {"a": 18, "b": "a"}}}
        cfg = XConfig(data=data)
        cfg.deep_update(other)
        expected = deepcopy(data)
        expected["c"]["b"][0]["a"] = 2
        assert not DeepDiff(cfg.decode(), expected)

    def test_full_merge(self):
        data = {
            "a": {"b": 10},
            "b": [0, 2, 1.02],
            "c": {"a": "b", "b": [{"a": 1, "b": 2}, "a"]},
        }
        other = {"c": {"b": [{"a": 2}], "e": {"a": 18, "b": "a"}}}
        cfg = XConfig(data=data)
        cfg.deep_update(other, full_merge=True)
        expected = {
            "a": {"b": 10},
            "b": [0, 2, 1.02],
            "c": {"a": "b", "b": [{"a": 2, "b": 2}, "a"], "e": {"a": 18, "b": "a"}},
        }
        assert not DeepDiff(cfg.decode(), expected)

    def test_walk(self, choixe_plain_cfg: Path):
        cfg = XConfig.from_file(choixe_plain_cfg)
        assert not DeepDiff(cfg.walk(), walk(parse(load(choixe_plain_cfg))))

    def test_process(self, choixe_plain_cfg: Path):
        cfg = XConfig.from_file(choixe_plain_cfg)
        processed = cfg.process().decode()
        processed_expected = process(
            parse(load(choixe_plain_cfg)), allow_branching=False
        )[0]
        assert not DeepDiff(processed, processed_expected)

    def test_process_all(self, choixe_plain_cfg: Path):
        cfg = XConfig.from_file(choixe_plain_cfg)
        processed = cfg.process_all()
        processed_expected = process(
            parse(load(choixe_plain_cfg)), allow_branching=True
        )
        for a, b in zip(processed, processed_expected):
            assert not DeepDiff(a.decode(), b)

    def test_inspect(self, choixe_plain_cfg: Path):
        cfg = XConfig.from_file(choixe_plain_cfg)
        inspection = cfg.inspect()
        expected = Inspection(processed=True)
        assert inspection == expected
