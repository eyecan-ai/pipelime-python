from typing import Any

import pytest

import pipelime.choixe.ast.nodes as ast
from pipelime.choixe.ast.parser import ChoixeParsingError, parse


class TestParse:
    def test_parse_dict(self):
        expr = {"a": 10, "b": {"c": 10.0, "d": "hello"}}
        expected = ast.DictNode(
            nodes={
                ast.LiteralNode(data="a"): ast.LiteralNode(data=10),
                ast.LiteralNode(data="b"): ast.DictNode(
                    nodes={
                        ast.LiteralNode(data="c"): ast.LiteralNode(data=10.0),
                        ast.LiteralNode(data="d"): ast.LiteralNode(data="hello"),
                    }
                ),
            }
        )
        assert parse(expr) == expected

    def test_parse_list(self):
        expr = [1, 2, 3, ("foo", "bar", [10.0, 10])]
        expected = ast.ListNode(
            ast.LiteralNode(data=1),
            ast.LiteralNode(data=2),
            ast.LiteralNode(data=3),
            ast.ListNode(
                ast.LiteralNode(data="foo"),
                ast.LiteralNode(data="bar"),
                ast.ListNode(ast.LiteralNode(data=10.0), ast.LiteralNode(data=10)),
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
            symbol=ast.LiteralNode(data="path/to/a/script.py:ClassName"),
            args=ast.DictNode(
                nodes={
                    ast.LiteralNode(data="a"): ast.LiteralNode(data=10),
                    ast.LiteralNode(data="b"): ast.InstanceNode(
                        symbol=ast.LiteralNode(data="some.interesting.module.MyClass"),
                        args=ast.DictNode(
                            nodes={
                                ast.LiteralNode(data="foo"): ast.LiteralNode(
                                    data="hello"
                                ),
                                ast.LiteralNode(data="bar"): ast.LiteralNode(
                                    data="world"
                                ),
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
            symbol=ast.LiteralNode(data="path/to/a/script.py:ModelName"),
            args=ast.DictNode(
                nodes={
                    ast.LiteralNode(data="a"): ast.LiteralNode(data=10),
                    ast.LiteralNode(data="b"): ast.DictNode(
                        nodes={
                            ast.LiteralNode(data="foo"): ast.LiteralNode(data="hello"),
                            ast.LiteralNode(data="bar"): ast.LiteralNode(data="world"),
                        }
                    ),
                }
            ),
        )
        assert parse(expr) == expected

    def test_parse_instance_no_args(self):
        expr = {"$call": "path/to/a/script.py:ClassName"}
        expected = ast.InstanceNode(
            symbol=ast.LiteralNode(data="path/to/a/script.py:ClassName"),
            args=ast.DictNode(nodes={}),
        )
        assert parse(expr) == expected

    def test_parse_model_no_args(self):
        expr = {"$model": "path/to/a/script.py:ModelName"}
        expected = ast.ModelNode(
            symbol=ast.LiteralNode(data="path/to/a/script.py:ModelName"),
            args=ast.DictNode(nodes={}),
        )
        assert parse(expr) == expected

    def test_parse_for(self):
        expr = {"$for(iterable, x)": {"node_$index(x)": "Hello_$item(x)"}}
        expected = ast.ForNode(
            iterable=ast.LiteralNode(data="iterable"),
            body=ast.DictNode(
                nodes={
                    ast.StrBundleNode(
                        ast.LiteralNode(data="node_"),
                        ast.IndexNode(identifier=ast.LiteralNode(data="x")),
                    ): ast.StrBundleNode(
                        ast.LiteralNode(data="Hello_"),
                        ast.ItemNode(identifier=ast.LiteralNode(data="x")),
                    )
                }
            ),
            identifier=ast.LiteralNode(data="x"),
        )
        assert parse(expr) == expected

    def test_parse_for_compact(self):
        expr = {"$for(iterable)": {"node_$index": "Hello_$item"}}
        expected = ast.ForNode(
            iterable=ast.LiteralNode(data="iterable"),
            body=ast.DictNode(
                nodes={
                    ast.StrBundleNode(
                        ast.LiteralNode(data="node_"), ast.IndexNode()
                    ): ast.StrBundleNode(ast.LiteralNode(data="Hello_"), ast.ItemNode())
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
                iterable=ast.LiteralNode(data="alpha"),
                body=ast.DictNode(
                    nodes={
                        ast.StrBundleNode(
                            ast.LiteralNode(data="node_"), ast.IndexNode()
                        ): ast.StrBundleNode(
                            ast.LiteralNode(data="Hello_"), ast.ItemNode()
                        )
                    }
                ),
            ),
            ast.ForNode(
                iterable=ast.LiteralNode(data="beta"),
                body=ast.DictNode(
                    nodes={
                        ast.StrBundleNode(
                            ast.LiteralNode(data="node_"), ast.IndexNode()
                        ): ast.StrBundleNode(
                            ast.LiteralNode(data="Ciao_"), ast.ItemNode()
                        )
                    }
                ),
            ),
            ast.ForNode(
                iterable=ast.LiteralNode(data="gamma"),
                body=ast.DictNode(
                    nodes={
                        ast.StrBundleNode(
                            ast.LiteralNode(data="node_"), ast.IndexNode()
                        ): ast.StrBundleNode(
                            ast.LiteralNode(data="Hola_"), ast.ItemNode()
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
        expected = ast.DictBundleNode(
            ast.ForNode(
                iterable=ast.LiteralNode(data="alpha"),
                body=ast.DictNode(
                    nodes={
                        ast.StrBundleNode(
                            ast.LiteralNode(data="node_"), ast.IndexNode()
                        ): ast.StrBundleNode(
                            ast.LiteralNode(data="Hello_"), ast.ItemNode()
                        )
                    }
                ),
            ),
            ast.ForNode(
                iterable=ast.LiteralNode(data="beta"),
                body=ast.DictNode(
                    nodes={
                        ast.StrBundleNode(
                            ast.LiteralNode(data="node_"), ast.IndexNode()
                        ): ast.StrBundleNode(
                            ast.LiteralNode(data="Ciao_"), ast.ItemNode()
                        )
                    }
                ),
            ),
            ast.DictNode(
                nodes={
                    ast.LiteralNode(data="a"): ast.LiteralNode(data=10),
                    ast.LiteralNode(data="b"): ast.DictNode(
                        nodes={
                            ast.LiteralNode(data="c"): ast.LiteralNode(data=10.0),
                            ast.LiteralNode(data="d"): ast.LiteralNode(data="hello"),
                        }
                    ),
                }
            ),
        )
        assert parse(expr) == expected

    def test_parse_switch_base(self):
        expr = {
            "$switch(nation)": [
                {
                    "$case": ["UK", "USA", "Australia"],
                    "$then": "hello",
                },
                {
                    "$case": "Italy",
                    "$then": "ciao",
                },
                {
                    "$default": "*raise your hand*",
                },
            ]
        }
        expected = ast.SwitchNode(
            value=ast.LiteralNode(data="nation"),
            cases=[
                (
                    ast.ListNode(
                        ast.LiteralNode(data="UK"),
                        ast.LiteralNode(data="USA"),
                        ast.LiteralNode(data="Australia"),
                    ),
                    ast.LiteralNode(data="hello"),
                ),
                (
                    ast.LiteralNode(data="Italy"),
                    ast.LiteralNode(data="ciao"),
                ),
            ],
            default=ast.LiteralNode(data="*raise your hand*"),
        )
        print(parse(expr))
        assert parse(expr) == expected

    def test_parse_invalid_switch(self):
        expr = {
            "$switch(nation)": [
                {
                    "$case": ["UK", "USA", "Australia"],
                    "$then": "hello",
                },
                {
                    "invalid": "value",
                },
                {
                    "$default": "*raise your hand*",
                },
            ]
        }
        with pytest.raises(ChoixeParsingError):
            print(parse(expr))


class TestStringParse:
    def test_simple(self):
        expr = "I am a string"
        assert parse(expr) == ast.LiteralNode(data=expr)

    @pytest.mark.parametrize(
        ["id_", "default", "env", "help"],
        [
            ["var1", 10, False, "help"],
            ["var1.var2", "hello", True, ""],
            ["var.var.var", 10.0, True, "help with many words"],
        ],
    )
    def test_var(self, id_: Any, default: Any, env: bool, help: str):
        default_str = f'"{default}"' if isinstance(default, str) else default
        expr = f'$var({id_}, default={default_str}, env={env}, help="{help}")'
        print(expr)
        assert parse(expr) == ast.VarNode(
            identifier=ast.LiteralNode(data=id_),
            default=ast.LiteralNode(data=default),
            env=ast.LiteralNode(data=env),
            help=ast.LiteralNode(data=help),
        )

    def test_import(self):
        path = "path/to/my/file.json"
        expr = f"$import('{path}')"
        assert parse(expr) == ast.ImportNode(path=ast.LiteralNode(data=path))

    def test_sweep(self):
        expr = "$sweep(10, foo.bar, '30')"
        expected = ast.SweepNode(
            ast.LiteralNode(data=10),
            ast.LiteralNode(data="foo.bar"),
            ast.LiteralNode(data="30"),
        )
        assert parse(expr) == expected

    def test_str_bundle(self):
        expr = "I am a string with $var(one.two.three) and $sweep(10, foo.bar, '30')"
        expected = ast.StrBundleNode(
            ast.LiteralNode(data="I am a string with "),
            ast.VarNode(identifier=ast.LiteralNode(data="one.two.three")),
            ast.LiteralNode(data=" and "),
            ast.SweepNode(
                ast.LiteralNode(data=10),
                ast.LiteralNode(data="foo.bar"),
                ast.LiteralNode(data="30"),
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
        format_ = ast.LiteralNode(data=format_) if format_ is not None else None
        assert parse(expr) == ast.DateNode(format=format_)

    @pytest.mark.parametrize(["name"], [[None], ["my_name"]])
    def test_tmp(self, name):
        expr = "$tmp"
        if name is not None:
            expr += f'("{name}")'
        name = ast.LiteralNode(data=name) if name is not None else None
        assert parse(expr) == ast.TmpDirNode(name=name)

    @pytest.mark.parametrize(["symbol"], [["builtins.str"], ["np.array"]])
    def test_symbol(self, symbol: str) -> None:
        expr = f"$symbol({symbol})"
        assert parse(expr) == ast.SymbolNode(symbol=ast.LiteralNode(data=symbol))

    @pytest.mark.parametrize(
        ["expr", "expected"],
        [
            ["$rand", ast.RandNode()],
            ["$rand()", ast.RandNode()],
            ["$rand(10)", ast.RandNode(ast.LiteralNode(data=10))],
            [
                "$rand(10, 20)",
                ast.RandNode(ast.LiteralNode(data=10), ast.LiteralNode(data=20)),
            ],
            [
                "$rand(10, 20, n=10)",
                ast.RandNode(
                    ast.LiteralNode(data=10),
                    ast.LiteralNode(data=20),
                    n=ast.LiteralNode(data=10),
                ),
            ],
            [
                "$rand(10, 20, pdf=[10, 20, 5, 4])",
                ast.RandNode(
                    ast.LiteralNode(data=10),
                    ast.LiteralNode(data=20),
                    pdf=ast.ListNode(
                        ast.LiteralNode(data=10),
                        ast.LiteralNode(data=20),
                        ast.LiteralNode(data=5),
                        ast.LiteralNode(data=4),
                    ),
                ),
            ],
            [
                "$rand(10, 20, n=10, pdf=[[0.0, 0.1], [2.5, [1.0, 0.4]], [5.0, 2.0], [7.5, [0.0, 0.2]]])",
                ast.RandNode(
                    ast.LiteralNode(data=10),
                    ast.LiteralNode(data=20),
                    n=ast.LiteralNode(data=10),
                    pdf=ast.ListNode(
                        ast.ListNode(
                            ast.LiteralNode(data=0.0),
                            ast.LiteralNode(data=0.1),
                        ),
                        ast.ListNode(
                            ast.LiteralNode(data=2.5),
                            ast.ListNode(
                                ast.LiteralNode(data=1.0),
                                ast.LiteralNode(data=0.4),
                            ),
                        ),
                        ast.ListNode(
                            ast.LiteralNode(data=5.0),
                            ast.LiteralNode(data=2.0),
                        ),
                        ast.ListNode(
                            ast.LiteralNode(data=7.5),
                            ast.ListNode(
                                ast.LiteralNode(data=0.0),
                                ast.LiteralNode(data=0.2),
                            ),
                        ),
                    ),
                ),
            ],
            [
                "$rand(10, 20, n=10, pdf=lambda x: 10-x)",
                ast.RandNode(
                    ast.LiteralNode(data=10),
                    ast.LiteralNode(data=20),
                    n=ast.LiteralNode(data=10),
                    pdf=ast.LiteralNode(data=lambda x: 10 - x),
                ),
            ],
        ],
    )
    def test_rand(self, expr: str, expected: ast.RandNode) -> None:
        assert parse(expr) == expected


class TestParserRaise:
    def test_unknown_directive(self):
        expr = "$unknown_directive(lots, of, params=10)"
        with pytest.raises(ChoixeParsingError):
            parse(expr)

    # def test_arg_too_complex(self):
    #     expr = "$sweep(lots, of, {'arguments', '10'})"
    #     with pytest.raises(ChoixeParsingError):
    #         parse(expr)

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
