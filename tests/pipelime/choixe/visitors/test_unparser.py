from typing import Any

import pytest
from pipelime.choixe.ast.nodes import (
    CmdNode,
    DateNode,
    DictNode,
    ForNode,
    ImportNode,
    IndexNode,
    InstanceNode,
    ItemNode,
    ListNode,
    ModelNode,
    Node,
    LiteralNode,
    StrBundleNode,
    SweepNode,
    TmpDirNode,
    UuidNode,
    VarNode,
)
from pipelime.choixe.visitors import unparse
from deepdiff import DeepDiff


@pytest.mark.parametrize(
    ["node", "expected"],
    [
        [LiteralNode(10), 10],
        [LiteralNode(0.124), 0.124],
        [LiteralNode("hello"), "hello"],
        [LiteralNode("my_var"), "my_var"],
        [
            VarNode(LiteralNode("variable.one")),
            "$var(variable.one)",
        ],
        [
            VarNode(LiteralNode("variable.one"), env=LiteralNode(True)),
            "$var(variable.one, env=True)",
        ],
        [
            VarNode(LiteralNode("variable.one"), default=LiteralNode(-24)),
            "$var(variable.one, default=-24)",
        ],
        [
            VarNode(
                LiteralNode("variable.one"),
                default=LiteralNode(-24),
                env=LiteralNode(True),
            ),
            "$var(variable.one, default=-24, env=True)",
        ],
        [ImportNode(LiteralNode("path/to/file.yaml")), '$import("path/to/file.yaml")'],
        [
            SweepNode(LiteralNode("a"), LiteralNode("variable"), LiteralNode(10)),
            "$sweep(a, variable, 10)",
        ],
        [StrBundleNode(LiteralNode("alice")), "alice"],
        [
            StrBundleNode(
                LiteralNode("alice "),
                VarNode(LiteralNode("foo"), default=LiteralNode("loves")),
                LiteralNode(" bob"),
            ),
            "alice $var(foo, default=loves) bob",
        ],
        [
            DictNode(
                {
                    LiteralNode("key1"): LiteralNode(10),
                    LiteralNode("key2"): DictNode(
                        {
                            LiteralNode("key1"): LiteralNode(10.2),
                            LiteralNode("key2"): LiteralNode("hello"),
                        }
                    ),
                }
            ),
            {"key1": 10, "key2": {"key1": 10.2, "key2": "hello"}},
        ],
        [
            DictNode(
                {
                    StrBundleNode(
                        VarNode(LiteralNode("var")), LiteralNode("foo")
                    ): LiteralNode("bar")
                }
            ),
            {"$var(var)foo": "bar"},
        ],
        [
            ListNode(LiteralNode(10), LiteralNode(-0.25), ListNode(LiteralNode("aa"))),
            [10, -0.25, ["aa"]],
        ],
        [
            InstanceNode(
                LiteralNode("path/to_my/file.py:MyClass"),
                DictNode(
                    {
                        LiteralNode("arg1"): InstanceNode(
                            LiteralNode("module.submodule.function"),
                            DictNode(
                                {
                                    LiteralNode("a"): ListNode(
                                        LiteralNode(1), LiteralNode(2)
                                    ),
                                    LiteralNode("b"): LiteralNode(100),
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
            ModelNode(
                LiteralNode("path/to_my/file.py:MyModel"),
                DictNode(
                    {
                        LiteralNode("arg1"): DictNode(
                            {
                                LiteralNode("a"): ListNode(
                                    LiteralNode(1), LiteralNode(2)
                                ),
                                LiteralNode("b"): LiteralNode(100),
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
            ForNode(
                LiteralNode("my.var"),
                DictNode(
                    {
                        LiteralNode("Hello"): LiteralNode("World"),
                        StrBundleNode(
                            LiteralNode("Number_"), IndexNode(LiteralNode("x"))
                        ): ItemNode(LiteralNode("x")),
                    }
                ),
                LiteralNode("x"),
            ),
            {"$for(my.var, x)": {"Hello": "World", "Number_$index(x)": "$item(x)"}},
        ],
        [
            ForNode(
                LiteralNode("my.var"),
                DictNode(
                    {
                        LiteralNode("Hello"): LiteralNode("World"),
                        StrBundleNode(LiteralNode("Number_"), IndexNode()): ItemNode(),
                    }
                ),
            ),
            {"$for(my.var)": {"Hello": "World", "Number_$index": "$item"}},
        ],
        [UuidNode(), "$uuid"],
        [DateNode(), "$date"],
        [DateNode(LiteralNode("%Y%m%d")), '$date("%Y%m%d")'],
        [CmdNode(LiteralNode("ls -lha")), '$cmd("ls -lha")'],
        [TmpDirNode(), "$tmp"],
        [TmpDirNode(LiteralNode("my_tmp")), "$tmp(my_tmp)"],
    ],
)
def test_unparse(node: Node, expected: Any):
    assert not DeepDiff(unparse(node), expected)
