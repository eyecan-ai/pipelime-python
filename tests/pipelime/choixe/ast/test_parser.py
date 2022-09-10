from typing import Any

import pytest

import pipelime.choixe.ast.nodes as ast
from pipelime.choixe.ast.parser import ChoixeParsingError, parse


class TestParse:
    def test_parse_dict(self):
        expr = {"a": 10, "b": {"c": 10.0, "d": "hello"}}
        expected = ast.DictNode(
            {
                ast.LiteralNode("a"): ast.LiteralNode(10),
                ast.LiteralNode("b"): ast.DictNode(
                    {
                        ast.LiteralNode("c"): ast.LiteralNode(10.0),
                        ast.LiteralNode("d"): ast.LiteralNode("hello"),
                    }
                ),
            }
        )
        assert parse(expr) == expected

    def test_parse_list(self):
        expr = [1, 2, 3, ("foo", "bar", [10.0, 10])]
        expected = ast.ListNode(
            ast.LiteralNode(1),
            ast.LiteralNode(2),
            ast.LiteralNode(3),
            ast.ListNode(
                ast.LiteralNode("foo"),
                ast.LiteralNode("bar"),
                ast.ListNode(ast.LiteralNode(10.0), ast.LiteralNode(10)),
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
        expected = ast.InstanceNode(
            ast.LiteralNode("path/to/a/script.py:ClassName"),
            ast.DictNode(
                {
                    ast.LiteralNode("a"): ast.LiteralNode(10),
                    ast.LiteralNode("b"): ast.InstanceNode(
                        ast.LiteralNode("some.interesting.module.MyClass"),
                        ast.DictNode(
                            {
                                ast.LiteralNode("foo"): ast.LiteralNode("hello"),
                                ast.LiteralNode("bar"): ast.LiteralNode("world"),
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
        expected = ast.ModelNode(
            ast.LiteralNode("path/to/a/script.py:ModelName"),
            ast.DictNode(
                {
                    ast.LiteralNode("a"): ast.LiteralNode(10),
                    ast.LiteralNode("b"): ast.DictNode(
                        {
                            ast.LiteralNode("foo"): ast.LiteralNode("hello"),
                            ast.LiteralNode("bar"): ast.LiteralNode("world"),
                        }
                    ),
                }
            ),
        )
        assert parse(expr) == expected

    def test_parse_instance_no_args(self):
        expr = {"$call": "path/to/a/script.py:ClassName"}
        expected = ast.InstanceNode(
            ast.LiteralNode("path/to/a/script.py:ClassName"),
            ast.DictNode({}),
        )
        assert parse(expr) == expected

    def test_parse_model_no_args(self):
        expr = {"$model": "path/to/a/script.py:ModelName"}
        expected = ast.ModelNode(
            ast.LiteralNode("path/to/a/script.py:ModelName"),
            ast.DictNode({}),
        )
        assert parse(expr) == expected

    def test_parse_for(self):
        expr = {"$for(iterable, x)": {"node_$index(x)": "Hello_$item(x)"}}
        expected = ast.ForNode(
            ast.LiteralNode("iterable"),
            ast.DictNode(
                {
                    ast.StrBundleNode(
                        ast.LiteralNode("node_"), ast.IndexNode(ast.LiteralNode("x"))
                    ): ast.StrBundleNode(
                        ast.LiteralNode("Hello_"), ast.ItemNode(ast.LiteralNode("x"))
                    )
                }
            ),
            ast.LiteralNode("x"),
        )
        assert parse(expr) == expected

    def test_parse_for_compact(self):
        expr = {"$for(iterable)": {"node_$index": "Hello_$item"}}
        expected = ast.ForNode(
            ast.LiteralNode("iterable"),
            ast.DictNode(
                {
                    ast.StrBundleNode(
                        ast.LiteralNode("node_"), ast.IndexNode()
                    ): ast.StrBundleNode(ast.LiteralNode("Hello_"), ast.ItemNode())
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
        expected = ast.DictBundleNode(
            ast.ForNode(
                ast.LiteralNode("alpha"),
                ast.DictNode(
                    {
                        ast.StrBundleNode(
                            ast.LiteralNode("node_"), ast.IndexNode()
                        ): ast.StrBundleNode(ast.LiteralNode("Hello_"), ast.ItemNode())
                    }
                ),
            ),
            ast.ForNode(
                ast.LiteralNode("beta"),
                ast.DictNode(
                    {
                        ast.StrBundleNode(
                            ast.LiteralNode("node_"), ast.IndexNode()
                        ): ast.StrBundleNode(ast.LiteralNode("Ciao_"), ast.ItemNode())
                    }
                ),
            ),
            ast.ForNode(
                ast.LiteralNode("gamma"),
                ast.DictNode(
                    {
                        ast.StrBundleNode(
                            ast.LiteralNode("node_"), ast.IndexNode()
                        ): ast.StrBundleNode(ast.LiteralNode("Hola_"), ast.ItemNode())
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
        expected = ast.DictBundleNode(
            ast.ForNode(
                ast.LiteralNode("alpha"),
                ast.DictNode(
                    {
                        ast.StrBundleNode(
                            ast.LiteralNode("node_"), ast.IndexNode()
                        ): ast.StrBundleNode(ast.LiteralNode("Hello_"), ast.ItemNode())
                    }
                ),
            ),
            ast.ForNode(
                ast.LiteralNode("beta"),
                ast.DictNode(
                    {
                        ast.StrBundleNode(
                            ast.LiteralNode("node_"), ast.IndexNode()
                        ): ast.StrBundleNode(ast.LiteralNode("Ciao_"), ast.ItemNode())
                    }
                ),
            ),
            ast.DictNode(
                {
                    ast.LiteralNode("a"): ast.LiteralNode(10),
                    ast.LiteralNode("b"): ast.DictNode(
                        {
                            ast.LiteralNode("c"): ast.LiteralNode(10.0),
                            ast.LiteralNode("d"): ast.LiteralNode("hello"),
                        }
                    ),
                }
            ),
        )
        assert parse(expr) == expected


class TestStringParse:
    def test_simple(self):
        expr = "I am a string"
        assert parse(expr) == ast.LiteralNode(expr)

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
        assert parse(expr) == ast.VarNode(
            ast.LiteralNode(id_),
            default=ast.LiteralNode(default),
            env=ast.LiteralNode(env),
        )

    def test_import(self):
        path = "path/to/my/file.json"
        expr = f"$import('{path}')"
        assert parse(expr) == ast.ImportNode(ast.LiteralNode(path))

    def test_sweep(self):
        expr = "$sweep(10, foo.bar, '30')"
        expected = ast.SweepNode(
            ast.LiteralNode(10), ast.LiteralNode("foo.bar"), ast.LiteralNode("30")
        )
        assert parse(expr) == expected

    def test_str_bundle(self):
        expr = "I am a string with $var(one.two.three) and $sweep(10, foo.bar, '30')"
        expected = ast.StrBundleNode(
            ast.LiteralNode("I am a string with "),
            ast.VarNode(ast.LiteralNode("one.two.three")),
            ast.LiteralNode(" and "),
            ast.SweepNode(
                ast.LiteralNode(10), ast.LiteralNode("foo.bar"), ast.LiteralNode("30")
            ),
        )
        assert parse(expr) == expected

    def test_uuid(self):
        expr = "$uuid"
        expected = ast.UuidNode()
        assert parse(expr) == expected

    @pytest.mark.parametrize(["format_"], [[None], ["%Y-%m-%d"], ["%h:%M:%s"]])
    def test_date(self, format_):
        expr = "$date"
        if format_ is not None:
            expr += f'("{format_}")'
        format_ = ast.LiteralNode(format_) if format_ is not None else None
        assert parse(expr) == ast.DateNode(format_)

    @pytest.mark.parametrize(["name"], [[None], ["my_name"]])
    def test_tmp(self, name):
        expr = "$tmp"
        if name is not None:
            expr += f'("{name}")'
        name = ast.LiteralNode(name) if name is not None else None
        assert parse(expr) == ast.TmpDirNode(name)

    @pytest.mark.parametrize(["symbol"], [["builtins.str"], ["np.array"]])
    def test_symbol(self, symbol: str) -> None:
        expr = f"$symbol({symbol})"
        assert parse(expr) == ast.SymbolNode(ast.LiteralNode(symbol))


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

    def test_var_inline_list_without_quotes(self):
        from yaml import safe_load
        from io import StringIO

        # Never do this btw
        yaml_expr = """a: [ $var(one.two.three, default=10) ]"""

        loaded = safe_load(StringIO(yaml_expr))
        with pytest.raises(ChoixeParsingError):
            parse(loaded)
