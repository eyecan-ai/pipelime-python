import pydash as py_
import pytest

from pipelime.choixe import XConfig
from pipelime.choixe.utils.common import deep_set_, pydash_path


class TestPydashPath:
    """String key paths address the same value on every pydash version: pydash 8.1
    keeps the empty keys of a leading, trailing or doubled `.` (`".a"` -> `["", "a"]`),
    where 8.0.x dropped them."""

    @pytest.mark.parametrize(
        "path,expected",
        [
            ("a.b", "a.b"),
            (".a.b", "a.b"),
            ("a..b.", "a.b"),
            ("a[1]..b", "a[1].b"),
            (".[0][1]", "[0][1]"),
            ("a[-1].", "a[-1]"),
            (r".a\.b..c", r"a\.b.c"),
            ("[x.[2]x", "[x[2].x"),  # not an index: a key
            (".a", "a"),  # a single key: pydash takes it as is
            ("\\\\x\\\\.", "\\x"),  # ...so it is unescaped here
            (".", []),
            ("..", []),
        ],
    )
    def test_string_paths(self, path, expected):
        assert pydash_path(path) == expected

    @pytest.mark.parametrize("path", ["a", "", 3, ["a", "", "b"], ("a",)])
    def test_other_paths_untouched(self, path):
        assert pydash_path(path) is path

    def test_get_and_set(self):
        data = {"a": {"b": [0, {"c": 1}], "x.y": 2}}
        assert py_.get(100, pydash_path(".")) == 100
        assert py_.get(data, pydash_path(".a.b[1].c")) == 1
        assert py_.get(data, pydash_path(r"a..x\.y")) == 2
        out = {}
        py_.set_(out, pydash_path(".a.b[1].c"), 5)
        assert out == {"a": {"b": [None, {"c": 5}]}}

    def test_deep_set_(self):
        out = {}
        deep_set_(out, ".a..b[1]", 5)
        assert out == {"a": {"b": [None, 5]}}

    def test_xconfig(self):
        cfg = XConfig({"a": {"b": 1}})
        assert cfg.deep_get(".a.b") == 1
        cfg.deep_set("a..b", 2)
        assert cfg.to_dict() == {"a": {"b": 2}}
