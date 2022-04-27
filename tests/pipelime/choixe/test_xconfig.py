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
        assert not DeepDiff(cfg_copy.to_dict(), cfg.to_dict())
        assert cfg_copy.get_schema() == cfg.get_schema()
        assert cfg_copy.get_cwd() == cfg.get_cwd()

    def test_from_file(self, choixe_plain_cfg: Path):
        cfg = XConfig.from_file(choixe_plain_cfg)
        assert not DeepDiff(cfg.to_dict(), load(choixe_plain_cfg))
        assert cfg.get_cwd() == choixe_plain_cfg.parent
        self._copy_test(cfg)

    def test_from_dict(self, choixe_plain_cfg: Path):
        data = load(choixe_plain_cfg)
        cfg = XConfig(data=data)
        assert not DeepDiff(cfg.to_dict(), data)
        assert cfg.get_cwd() is None
        self._copy_test(cfg)

    def test_from_nothing(self):
        cfg = XConfig()
        assert not DeepDiff(cfg.to_dict(), {})
        assert cfg.get_cwd() is None
        self._copy_test(cfg)

    def test_file_io(self, tmp_path: Path, choixe_plain_cfg: Path):
        cfg = XConfig.from_file(choixe_plain_cfg)
        save_path = tmp_path / "config.yml"
        cfg.save_to(save_path)
        re_cfg = XConfig.from_file(save_path)

        assert re_cfg.get_cwd() == save_path.parent
        assert not DeepDiff(re_cfg.to_dict(), cfg.to_dict())

    def test_with_schema(self, choixe_plain_cfg: Path):
        cfg = XConfig.from_file(choixe_plain_cfg)
        assert cfg.is_valid()  # No schema: always valid

        schema = Schema(
            {"alice": int, "bob": int, "charlie": [Or(str, {str: Use(int)})]}
        )
        cfg.set_schema(schema)
        assert cfg.get_schema() == schema
        assert cfg.is_valid()
        cfg.validate()
        expected = schema.validate(load(choixe_plain_cfg))
        assert not DeepDiff(cfg.to_dict(), expected)

    def test_deep_keys(self, choixe_plain_cfg: Path):
        cfg = XConfig.from_file(choixe_plain_cfg)
        assert cfg.deep_get("charlie.2.alpha") == 10.0
        assert cfg.deep_get(["charlie", 2, "beta"]) == 20.0
        assert cfg.deep_get("bob.alpha", default="hello") == "hello"

        cfg.deep_set("charlie.2.alpha", 40, only_valid_keys=False)
        assert cfg.deep_get("charlie.2.alpha") == 40

        cfg.deep_set("charlie.2.alpha", 50, only_valid_keys=True)
        assert cfg.deep_get("charlie.2.alpha") == 50

        cfg.deep_set("charlie.3.foo.bar", [10, 20, 30], only_valid_keys=False)
        assert cfg.deep_get("charlie.3") == {"foo": {"bar": [10, 20, 30]}}

        cfg.deep_set("charlie.4.foo.bar", [10, 20, 30], only_valid_keys=True)
        assert cfg.deep_get("charlie.4") is None

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
        assert not DeepDiff(cfg.to_dict(), expected)

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
        assert not DeepDiff(cfg.to_dict(), expected)

    def test_walk(self, choixe_plain_cfg: Path):
        cfg = XConfig.from_file(choixe_plain_cfg)
        assert not DeepDiff(cfg.walk(), walk(parse(load(choixe_plain_cfg))))

    def test_process(self, choixe_plain_cfg: Path):
        cfg = XConfig.from_file(choixe_plain_cfg)
        processed = cfg.process().to_dict()
        processed_expected = process(parse(load(choixe_plain_cfg)), allow_branching=False)[0]
        assert not DeepDiff(processed, processed_expected)

    def test_process_all(self, choixe_plain_cfg: Path):
        cfg = XConfig.from_file(choixe_plain_cfg)
        processed = cfg.process_all()
        processed_expected = process(parse(load(choixe_plain_cfg)), allow_branching=True)
        for a, b in zip(processed, processed_expected):
            assert not DeepDiff(a.to_dict(), b)

    def test_inspect(self, choixe_plain_cfg: Path):
        cfg = XConfig.from_file(choixe_plain_cfg)
        inspection = cfg.inspect()
        expected = Inspection(processed=True)
        assert inspection == expected
