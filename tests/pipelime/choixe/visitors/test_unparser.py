from typing import Any

import pytest
from deepdiff import DeepDiff

import pipelime.choixe.ast.nodes as ast
from pipelime.choixe.visitors import unparse


@pytest.mark.parametrize(
    ["node", "expected"],
    [
        [ast.LiteralNode(data=10), 10],
        [ast.LiteralNode(data=0.124), 0.124],
        [ast.LiteralNode(data="hello"), "hello"],
        [ast.LiteralNode(data="my_var"), "my_var"],
        [
            ast.VarNode(identifier=ast.LiteralNode(data="variable.one")),
            "$var(variable.one)",
        ],
        [
            ast.VarNode(
                identifier=ast.LiteralNode(data="variable.one"),
                env=ast.LiteralNode(data=True),
            ),
            "$var(variable.one, env=True)",
        ],
        [
            ast.VarNode(
                identifier=ast.LiteralNode(data="variable.one"),
                default=ast.LiteralNode(data=-24),
            ),
            "$var(variable.one, default=-24)",
        ],
        [
            ast.VarNode(
                identifier=ast.LiteralNode(data="variable.one"),
                default=ast.LiteralNode(data=-24),
                env=ast.LiteralNode(data=True),
            ),
            "$var(variable.one, default=-24, env=True)",
        ],
        [
            ast.VarNode(
                identifier=ast.LiteralNode(data="variable"),
                default=ast.LiteralNode(data=32),
                help=ast.LiteralNode(data="help message"),
            ),
            '$var(variable, default=32, help="help message")',
        ],
        [
            ast.ImportNode(path=ast.LiteralNode(data="path/to/file.yaml")),
            '$import("path/to/file.yaml")',
        ],
        [
            ast.SweepNode(
                ast.LiteralNode(data="a"),
                ast.LiteralNode(data="variable"),
                ast.LiteralNode(data=10),
            ),
            "$sweep(a, variable, 10)",
        ],
        [ast.StrBundleNode(ast.LiteralNode(data="alice")), "alice"],
        [
            ast.StrBundleNode(
                ast.LiteralNode(data="alice "),
                ast.VarNode(
                    identifier=ast.LiteralNode(data="foo"),
                    default=ast.LiteralNode(data="loves"),
                ),
                ast.LiteralNode(data=" bob"),
            ),
            "alice $var(foo, default=loves) bob",
        ],
        [
            ast.DictNode(
                nodes={
                    ast.LiteralNode(data="key1"): ast.LiteralNode(data=10),
                    ast.LiteralNode(data="key2"): ast.DictNode(
                        nodes={
                            ast.LiteralNode(data="key1"): ast.LiteralNode(data=10.2),
                            ast.LiteralNode(data="key2"): ast.LiteralNode(data="hello"),
                        }
                    ),
                }
            ),
            {"key1": 10, "key2": {"key1": 10.2, "key2": "hello"}},
        ],
        [
            ast.DictNode(
                nodes={
                    ast.StrBundleNode(
                        ast.VarNode(identifier=ast.LiteralNode(data="var")),
                        ast.LiteralNode(data="foo"),
                    ): ast.LiteralNode(data="bar")
                }
            ),
            {"$var(var)foo": "bar"},
        ],
        [
            ast.ListNode(
                ast.LiteralNode(data=10),
                ast.LiteralNode(data=-0.25),
                ast.ListNode(ast.LiteralNode(data="aa")),
            ),
            [10, -0.25, ["aa"]],
        ],
        [
            ast.SymbolNode(symbol=ast.LiteralNode(data="numpy.zeros")),
            "$symbol(numpy.zeros)",
        ],
        [
            ast.InstanceNode(
                symbol=ast.LiteralNode(data="path/to_my/file.py:MyClass"),
                args=ast.DictNode(
                    nodes={
                        ast.LiteralNode(data="arg1"): ast.InstanceNode(
                            symbol=ast.LiteralNode(data="module.submodule.function"),
                            args=ast.DictNode(
                                nodes={
                                    ast.LiteralNode(data="a"): ast.ListNode(
                                        ast.LiteralNode(data=1), ast.LiteralNode(data=2)
                                    ),
                                    ast.LiteralNode(data="b"): ast.LiteralNode(
                                        data=100
                                    ),
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
                symbol=ast.LiteralNode(data="path/to_my/file.py:MyModel"),
                args=ast.DictNode(
                    nodes={
                        ast.LiteralNode(data="arg1"): ast.DictNode(
                            nodes={
                                ast.LiteralNode(data="a"): ast.ListNode(
                                    ast.LiteralNode(data=1), ast.LiteralNode(data=2)
                                ),
                                ast.LiteralNode(data="b"): ast.LiteralNode(data=100),
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
                iterable=ast.LiteralNode(data="my.var"),
                body=ast.DictNode(
                    nodes={
                        ast.LiteralNode(data="Hello"): ast.LiteralNode(data="World"),
                        ast.StrBundleNode(
                            ast.LiteralNode(data="Number_"),
                            ast.IndexNode(identifier=ast.LiteralNode(data="x")),
                        ): ast.ItemNode(identifier=ast.LiteralNode(data="x")),
                    }
                ),
                identifier=ast.LiteralNode(data="x"),
            ),
            {"$for(my.var, x)": {"Hello": "World", "Number_$index(x)": "$item(x)"}},
        ],
        [
            ast.ForNode(
                iterable=ast.LiteralNode(data="my.var"),
                body=ast.DictNode(
                    nodes={
                        ast.LiteralNode(data="Hello"): ast.LiteralNode(data="World"),
                        ast.StrBundleNode(
                            ast.LiteralNode(data="Number_"), ast.IndexNode()
                        ): ast.ItemNode(),
                    }
                ),
            ),
            {"$for(my.var)": {"Hello": "World", "Number_$index()": "$item"}},
        ],
        [
            ast.SwitchNode(
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
            ),
            {
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
            },
        ],
        [ast.UuidNode(), "$uuid"],
        [ast.DateNode(), "$date"],
        [ast.DateNode(format=ast.LiteralNode(data="%Y%m%d")), '$date("%Y%m%d")'],
        [ast.CmdNode(command=ast.LiteralNode(data="ls -lha")), '$cmd("ls -lha")'],
        [ast.TmpDirNode(), "$tmp"],
        [ast.TmpDirNode(name=ast.LiteralNode(data="my_tmp")), "$tmp(my_tmp)"],
        [
            ast.DictBundleNode(
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
                                ast.LiteralNode(data="d"): ast.LiteralNode(
                                    data="hello"
                                ),
                            }
                        ),
                    }
                ),
            ),
            {
                "$for(alpha)": {"node_$index()": "Hello_$item()"},
                "$for(beta)": {"node_$index()": "Ciao_$item()"},
                "a": 10,
                "b": {"c": 10.0, "d": "hello"},
            },
        ],
        [ast.RandNode(), "$rand"],
        [ast.RandNode(ast.LiteralNode(data=10)), "$rand(10)"],
        [
            ast.RandNode(ast.LiteralNode(data=10), ast.LiteralNode(data=20)),
            "$rand(10, 20)",
        ],
        [
            ast.RandNode(
                ast.LiteralNode(data=10),
                ast.LiteralNode(data=20),
                n=ast.LiteralNode(data=10),
            ),
            "$rand(10, 20, n=10)",
        ],
        [
            ast.RandNode(
                ast.LiteralNode(data=10),
                ast.LiteralNode(data=20),
                pdf=ast.LiteralNode(data=[10, 20, 5, 4]),
            ),
            "$rand(10, 20, pdf=[10, 20, 5, 4])",
        ],
        [
            ast.RandNode(
                ast.LiteralNode(data=10),
                ast.LiteralNode(data=20),
                n=ast.LiteralNode(data=10),
                pdf=ast.LiteralNode(
                    data=[
                        [0.0, 0.1],
                        [2.5, [1.0, 0.4]],
                        [5.0, 2.0],
                        [7.5, [0.0, 0.2]],
                    ]
                ),
            ),
            "$rand(10, 20, n=10, pdf=[[0.0, 0.1], [2.5, [1.0, 0.4]], [5.0, 2.0], [7.5, [0.0, 0.2]]])",
        ],
        [
            ast.DictNode(
                nodes={
                    ast.LiteralNode(data="a"): ast.LiteralNode(data=10),
                    ast.LiteralNode(data=1): ast.DictNode(
                        nodes={
                            ast.LiteralNode(data=4): ast.LiteralNode(data=10.0),
                            ast.LiteralNode(data=10): ast.LiteralNode(data="hello"),
                        }
                    ),
                    ast.LiteralNode(data=10): ast.ListNode(
                        ast.LiteralNode(data=1),
                        ast.LiteralNode(data=2),
                        ast.LiteralNode(data=3),
                    ),
                }
            ),
            {"a": 10, 1: {4: 10.0, 10: "hello"}, 10: [1, 2, 3]},
        ],
    ],
)
def test_unparse(node: ast.Node, expected: Any):
    assert not DeepDiff(unparse(node), expected)
