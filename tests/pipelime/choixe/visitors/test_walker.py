from typing import Any

import pytest
from deepdiff import DeepDiff

from pipelime.choixe.ast.nodes import (
    DictNode,
    ListNode,
    LiteralNode,
    Node,
    StrBundleNode,
    VarNode,
)
from pipelime.choixe.visitors import walk


@pytest.mark.parametrize(
    ["node", "expected"],
    [
        [LiteralNode(data=10), [([], 10)]],
        [
            StrBundleNode(
                LiteralNode(data="alice "),
                VarNode(
                    identifier=LiteralNode(data="foo"),
                    default=LiteralNode(data="loves"),
                ),
                LiteralNode(data=" bob"),
            ),
            [([], "alice $var(foo, default=loves) bob")],
        ],
        [
            DictNode(
                nodes={
                    LiteralNode(data="key1"): LiteralNode(data=10),
                    LiteralNode(data="key2"): DictNode(
                        nodes={
                            LiteralNode(data="key1"): LiteralNode(data=10.2),
                            LiteralNode(data="key2"): LiteralNode(data="hello"),
                        }
                    ),
                }
            ),
            [(["key1"], 10), (["key2", "key1"], 10.2), (["key2", "key2"], "hello")],
        ],
        [
            ListNode(
                LiteralNode(data=10),
                LiteralNode(data=-0.25),
                ListNode(LiteralNode(data="aa")),
            ),
            [([0], 10), ([1], -0.25), ([2, 0], "aa")],
        ],
        [
            ListNode(
                LiteralNode(data=10),
                LiteralNode(data=-0.25),
                DictNode(
                    nodes={
                        LiteralNode(data=10): LiteralNode(data=10.2),
                        LiteralNode(data=20): LiteralNode(data="hello"),
                    }
                ),
            ),
            [([0], 10), ([1], -0.25), ([2, 10], 10.2), ([2, 20], "hello")],
        ],
        [
            DictNode(
                nodes={
                    LiteralNode(data="key1"): LiteralNode(data=10),
                    LiteralNode(data=20): LiteralNode(data=20),
                    LiteralNode(data=0.5): LiteralNode(data=30),
                    LiteralNode(data=b"hello"): LiteralNode(data=40),
                    LiteralNode(data=None): LiteralNode(data=50),
                    LiteralNode(data=True): LiteralNode(data=60),
                }
            ),
            [
                (["key1"], 10),
                ([20], 20),
                ([0.5], 30),
                ([b"hello"], 40),
                ([None], 50),
                ([True], 60),
            ],
        ],
        [
            DictNode(
                nodes={
                    LiteralNode(data="key1"): ListNode(
                        DictNode(
                            nodes={
                                LiteralNode(data=0.5): LiteralNode(data=10.2),
                                LiteralNode(data=20): LiteralNode(data="hello"),
                            }
                        ),
                        LiteralNode(data=10),
                    ),
                    LiteralNode(data=20): LiteralNode(data=20),
                }
            ),
            [
                (["key1", 0, 0.5], 10.2),
                (["key1", 0, 20], "hello"),
                (["key1", 1], 10),
                ([20], 20),
            ],
        ],
    ],
)
def test_walk(node: Node, expected: Any):
    assert not DeepDiff(walk(node), expected)
