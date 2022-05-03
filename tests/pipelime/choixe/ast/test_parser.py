from typing import Any

import pytest

from pipelime.choixe.ast.nodes import (
    DateNode,
    DictBundleNode,
    DictNode,
    ForNode,
    ImportNode,
    IndexNode,
    InstanceNode,
    ItemNode,
    ListNode,
    LiteralNode,
    ModelNode,
    StrBundleNode,
    SweepNode,
    TmpDirNode,
    UuidNode,
    VarNode,
)
from pipelime.choixe.ast.parser import ChoixeParsingError, parse


class TestParse:
    def test_parse_dict(self):
        expr = {"a": 10, "b": {"c": 10.0, "d": "hello"}}
        expected = DictNode(
            {
                LiteralNode("a"): LiteralNode(10),
                LiteralNode("b"): DictNode(
                    {
                        LiteralNode("c"): LiteralNode(10.0),
                        LiteralNode("d"): LiteralNode("hello"),
                    }
                ),
            }
        )
        assert parse(expr) == expected

    def test_parse_list(self):
        expr = [1, 2, 3, ("foo", "bar", [10.0, 10])]
        expected = ListNode(
            LiteralNode(1),
            LiteralNode(2),
            LiteralNode(3),
            ListNode(
                LiteralNode("foo"),
                LiteralNode("bar"),
                ListNode(LiteralNode(10.0), LiteralNode(10)),
            ),
        )
        assert parse(expr) == expected

    def test_parse_instance(self):
        expr = {
            "$call": "path/to/a/script.py:ClassName",
            "$args": {
                "a": 10,
                "b": {
                    "$call": "some.interesting.module.MyClass",
                    "$args": {
                        "foo": "hello",
                        "bar": "world",
                    },
                },
            },
        }
        expected = InstanceNode(
            LiteralNode("path/to/a/script.py:ClassName"),
            DictNode(
                {
                    LiteralNode("a"): LiteralNode(10),
                    LiteralNode("b"): InstanceNode(
                        LiteralNode("some.interesting.module.MyClass"),
                        DictNode(
                            {
                                LiteralNode("foo"): LiteralNode("hello"),
                                LiteralNode("bar"): LiteralNode("world"),
                            }
                        ),
                    ),
                }
            ),
        )
        assert parse(expr) == expected

    def test_parse_model(self):
        expr = {
            "$model": "path/to/a/script.py:ModelName",
            "$args": {
                "a": 10,
                "b": {
                    "foo": "hello",
                    "bar": "world",
                },
            },
        }
        expected = ModelNode(
            LiteralNode("path/to/a/script.py:ModelName"),
            DictNode(
                {
                    LiteralNode("a"): LiteralNode(10),
                    LiteralNode("b"): DictNode(
                        {
                            LiteralNode("foo"): LiteralNode("hello"),
                            LiteralNode("bar"): LiteralNode("world"),
                        }
                    ),
                }
            ),
        )
        assert parse(expr) == expected

    def test_parse_for(self):
        expr = {"$for(iterable, x)": {"node_$index(x)": "Hello_$item(x)"}}
        expected = ForNode(
            LiteralNode("iterable"),
            DictNode(
                {
                    StrBundleNode(
                        LiteralNode("node_"), IndexNode(LiteralNode("x"))
                    ): StrBundleNode(LiteralNode("Hello_"), ItemNode(LiteralNode("x")))
                }
            ),
            LiteralNode("x"),
        )
        assert parse(expr) == expected

    def test_parse_for_compact(self):
        expr = {"$for(iterable)": {"node_$index": "Hello_$item"}}
        expected = ForNode(
            LiteralNode("iterable"),
            DictNode(
                {
                    StrBundleNode(LiteralNode("node_"), IndexNode()): StrBundleNode(
                        LiteralNode("Hello_"), ItemNode()
                    )
                }
            ),
        )
        assert parse(expr) == expected

    def test_parse_for_multiple(self):
        expr = {
            "$for(alpha)": {"node_$index": "Hello_$item"},
            "$for(beta)": {"node_$index": "Ciao_$item"},
            "$for(gamma)": {"node_$index": "Hola_$item"},
        }
        expected = DictBundleNode(
            ForNode(
                LiteralNode("alpha"),
                DictNode(
                    {
                        StrBundleNode(LiteralNode("node_"), IndexNode()): StrBundleNode(
                            LiteralNode("Hello_"), ItemNode()
                        )
                    }
                ),
            ),
            ForNode(
                LiteralNode("beta"),
                DictNode(
                    {
                        StrBundleNode(LiteralNode("node_"), IndexNode()): StrBundleNode(
                            LiteralNode("Ciao_"), ItemNode()
                        )
                    }
                ),
            ),
            ForNode(
                LiteralNode("gamma"),
                DictNode(
                    {
                        StrBundleNode(LiteralNode("node_"), IndexNode()): StrBundleNode(
                            LiteralNode("Hola_"), ItemNode()
                        )
                    }
                ),
            ),
        )
        assert parse(expr) == expected

    def test_parse_for_multiple_mixed(self):
        expr = {
            "$for(alpha)": {"node_$index": "Hello_$item"},
            "$for(beta)": {"node_$index": "Ciao_$item"},
            "a": 10,
            "b": {"c": 10.0, "d": "hello"},
        }
        expected = DictBundleNode(
            ForNode(
                LiteralNode("alpha"),
                DictNode(
                    {
                        StrBundleNode(LiteralNode("node_"), IndexNode()): StrBundleNode(
                            LiteralNode("Hello_"), ItemNode()
                        )
                    }
                ),
            ),
            ForNode(
                LiteralNode("beta"),
                DictNode(
                    {
                        StrBundleNode(LiteralNode("node_"), IndexNode()): StrBundleNode(
                            LiteralNode("Ciao_"), ItemNode()
                        )
                    }
                ),
            ),
            DictNode(
                {
                    LiteralNode("a"): LiteralNode(10),
                    LiteralNode("b"): DictNode(
                        {
                            LiteralNode("c"): LiteralNode(10.0),
                            LiteralNode("d"): LiteralNode("hello"),
                        }
                    ),
                }
            ),
        )
        assert parse(expr) == expected


class TestStringParse:
    def test_simple(self):
        expr = "I am a string"
        assert parse(expr) == LiteralNode(expr)

    @pytest.mark.parametrize(
        ["id_", "default", "env"],
        [
            ["var1", 10, False],
            ["var1.var2", "hello", True],
            ["var.var.var", 10.0, True],
        ],
    )
    def test_var(self, id_: Any, default: Any, env: bool):
        default_str = f'"{default}"' if isinstance(default, str) else default
        expr = f"$var({id_}, default={default_str}, env={env})"
        assert parse(expr) == VarNode(
            LiteralNode(id_), default=LiteralNode(default), env=LiteralNode(env)
        )

    def test_import(self):
        path = "path/to/my/file.json"
        expr = f"$import('{path}')"
        assert parse(expr) == ImportNode(LiteralNode(path))

    def test_sweep(self):
        expr = "$sweep(10, foo.bar, '30')"
        expected = SweepNode(LiteralNode(10), LiteralNode("foo.bar"), LiteralNode("30"))
        assert parse(expr) == expected

    def test_str_bundle(self):
        expr = "I am a string with $var(one.two.three) and $sweep(10, foo.bar, '30')"
        expected = StrBundleNode(
            LiteralNode("I am a string with "),
            VarNode(LiteralNode("one.two.three")),
            LiteralNode(" and "),
            SweepNode(LiteralNode(10), LiteralNode("foo.bar"), LiteralNode("30")),
        )
        assert parse(expr) == expected

    def test_uuid(self):
        expr = "$uuid"
        expected = UuidNode()
        assert parse(expr) == expected

    @pytest.mark.parametrize(["format_"], [[None], ["%Y-%m-%d"], ["%h:%M:%s"]])
    def test_date(self, format_):
        expr = "$date"
        if format_ is not None:
            expr += f'("{format_}")'
        format_ = LiteralNode(format_) if format_ is not None else None
        assert parse(expr) == DateNode(format_)

    @pytest.mark.parametrize(["name"], [[None], ["my_name"]])
    def test_tmp(self, name):
        expr = "$tmp"
        if name is not None:
            expr += f'("{name}")'
        name = LiteralNode(name) if name is not None else None
        assert parse(expr) == TmpDirNode(name)


class TestParserRaise:
    def test_unknown_directive(self):
        expr = "$unknown_directive(lots, of, params=10)"
        with pytest.raises(ChoixeParsingError):
            parse(expr)

    def test_arg_too_complex(self):
        expr = "$sweep(lots, of, [arguments, '10'])"
        with pytest.raises(ChoixeParsingError):
            parse(expr)

    @pytest.mark.parametrize(
        ["expr"],
        [
            ["$var(f.dd111])"],
            ["I am a string with $import(ba[a)"],
            ["$var(invalid syntax ::) that raises syntaxerror"],
            ["$a+b a"],
        ],
    )
    def test_syntax_error(self, expr: str):
        with pytest.raises(ChoixeParsingError):
            parse(expr)

    def test_invalid_arg(self):
        expr = {"$directive": "var", "$args": ["$var(one.two)"], "$kwargs": {}}
        with pytest.raises(ChoixeParsingError):
            parse(expr)
