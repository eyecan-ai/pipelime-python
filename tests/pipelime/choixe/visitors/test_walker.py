from typing import Any

import pytest
from pipelime.choixe.ast.nodes import (
    DictNode,
    ListNode,
    Node,
    LiteralNode,
    StrBundleNode,
    VarNode,
)
from pipelime.choixe.visitors import walk
from deepdiff import DeepDiff


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
    ],
)
def test_walk(node: Node, expected: Any):
    assert not DeepDiff(walk(node), expected)
