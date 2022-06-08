from typing import Any

import pytest
from deepdiff import DeepDiff

import pipelime.choixe.ast.nodes as ast
from pipelime.choixe.visitors import unparse


@pytest.mark.parametrize(
    ["node", "expected"],
    [
        [ast.LiteralNode(10), 10],
        [ast.LiteralNode(0.124), 0.124],
        [ast.LiteralNode("hello"), "hello"],
        [ast.LiteralNode("my_var"), "my_var"],
        [
            ast.VarNode(ast.LiteralNode("variable.one")),
            "$var(variable.one)",
        ],
        [
            ast.VarNode(ast.LiteralNode("variable.one"), env=ast.LiteralNode(True)),
            "$var(variable.one, env=True)",
        ],
        [
            ast.VarNode(ast.LiteralNode("variable.one"), default=ast.LiteralNode(-24)),
            "$var(variable.one, default=-24)",
        ],
        [
            ast.VarNode(
                ast.LiteralNode("variable.one"),
                default=ast.LiteralNode(-24),
                env=ast.LiteralNode(True),
            ),
            "$var(variable.one, default=-24, env=True)",
        ],
        [
            ast.ImportNode(ast.LiteralNode("path/to/file.yaml")),
            '$import("path/to/file.yaml")',
        ],
        [
            ast.SweepNode(
                ast.LiteralNode("a"), ast.LiteralNode("variable"), ast.LiteralNode(10)
            ),
            "$sweep(a, variable, 10)",
        ],
        [ast.StrBundleNode(ast.LiteralNode("alice")), "alice"],
        [
            ast.StrBundleNode(
                ast.LiteralNode("alice "),
                ast.VarNode(ast.LiteralNode("foo"), default=ast.LiteralNode("loves")),
                ast.LiteralNode(" bob"),
            ),
            "alice $var(foo, default=loves) bob",
        ],
        [
            ast.DictNode(
                {
                    ast.LiteralNode("key1"): ast.LiteralNode(10),
                    ast.LiteralNode("key2"): ast.DictNode(
                        {
                            ast.LiteralNode("key1"): ast.LiteralNode(10.2),
                            ast.LiteralNode("key2"): ast.LiteralNode("hello"),
                        }
                    ),
                }
            ),
            {"key1": 10, "key2": {"key1": 10.2, "key2": "hello"}},
        ],
        [
            ast.DictNode(
                {
                    ast.StrBundleNode(
                        ast.VarNode(ast.LiteralNode("var")), ast.LiteralNode("foo")
                    ): ast.LiteralNode("bar")
                }
            ),
            {"$var(var)foo": "bar"},
        ],
        [
            ast.ListNode(
                ast.LiteralNode(10),
                ast.LiteralNode(-0.25),
                ast.ListNode(ast.LiteralNode("aa")),
            ),
            [10, -0.25, ["aa"]],
        ],
        [ast.SymbolNode(ast.LiteralNode("numpy.zeros")), "$symbol(numpy.zeros)"],
        [
            ast.InstanceNode(
                ast.LiteralNode("path/to_my/file.py:MyClass"),
                ast.DictNode(
                    {
                        ast.LiteralNode("arg1"): ast.InstanceNode(
                            ast.LiteralNode("module.submodule.function"),
                            ast.DictNode(
                                {
                                    ast.LiteralNode("a"): ast.ListNode(
                                        ast.LiteralNode(1), ast.LiteralNode(2)
                                    ),
                                    ast.LiteralNode("b"): ast.LiteralNode(100),
                                }
                            ),
                        )
                    }
                ),
            ),
            {
                "$call": "path/to_my/file.py:MyClass",
                "$args": {
                    "arg1": {
                        "$call": "module.submodule.function",
                        "$args": {"a": [1, 2], "b": 100},
                    }
                },
            },
        ],
        [
            ast.ModelNode(
                ast.LiteralNode("path/to_my/file.py:MyModel"),
                ast.DictNode(
                    {
                        ast.LiteralNode("arg1"): ast.DictNode(
                            {
                                ast.LiteralNode("a"): ast.ListNode(
                                    ast.LiteralNode(1), ast.LiteralNode(2)
                                ),
                                ast.LiteralNode("b"): ast.LiteralNode(100),
                            }
                        ),
                    }
                ),
            ),
            {
                "$model": "path/to_my/file.py:MyModel",
                "$args": {"arg1": {"a": [1, 2], "b": 100}},
            },
        ],
        [
            ast.ForNode(
                ast.LiteralNode("my.var"),
                ast.DictNode(
                    {
                        ast.LiteralNode("Hello"): ast.LiteralNode("World"),
                        ast.StrBundleNode(
                            ast.LiteralNode("Number_"),
                            ast.IndexNode(ast.LiteralNode("x")),
                        ): ast.ItemNode(ast.LiteralNode("x")),
                    }
                ),
                ast.LiteralNode("x"),
            ),
            {"$for(my.var, x)": {"Hello": "World", "Number_$index(x)": "$item(x)"}},
        ],
        [
            ast.ForNode(
                ast.LiteralNode("my.var"),
                ast.DictNode(
                    {
                        ast.LiteralNode("Hello"): ast.LiteralNode("World"),
                        ast.StrBundleNode(
                            ast.LiteralNode("Number_"), ast.IndexNode()
                        ): ast.ItemNode(),
                    }
                ),
            ),
            {"$for(my.var)": {"Hello": "World", "Number_$index": "$item"}},
        ],
        [ast.UuidNode(), "$uuid"],
        [ast.DateNode(), "$date"],
        [ast.DateNode(ast.LiteralNode("%Y%m%d")), '$date("%Y%m%d")'],
        [ast.CmdNode(ast.LiteralNode("ls -lha")), '$cmd("ls -lha")'],
        [ast.TmpDirNode(), "$tmp"],
        [ast.TmpDirNode(ast.LiteralNode("my_tmp")), "$tmp(my_tmp)"],
        [
            ast.DictBundleNode(
                ast.ForNode(
                    ast.LiteralNode("alpha"),
                    ast.DictNode(
                        {
                            ast.StrBundleNode(
                                ast.LiteralNode("node_"), ast.IndexNode()
                            ): ast.StrBundleNode(
                                ast.LiteralNode("Hello_"), ast.ItemNode()
                            )
                        }
                    ),
                ),
                ast.ForNode(
                    ast.LiteralNode("beta"),
                    ast.DictNode(
                        {
                            ast.StrBundleNode(
                                ast.LiteralNode("node_"), ast.IndexNode()
                            ): ast.StrBundleNode(
                                ast.LiteralNode("Ciao_"), ast.ItemNode()
                            )
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
            ),
            {
                "$for(alpha)": {"node_$index": "Hello_$item"},
                "$for(beta)": {"node_$index": "Ciao_$item"},
                "a": 10,
                "b": {"c": 10.0, "d": "hello"},
            },
        ],
    ],
)
def test_unparse(node: ast.Node, expected: Any):
    assert not DeepDiff(unparse(node), expected)
