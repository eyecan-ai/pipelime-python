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
        [LiteralNode(10), [([], 10)]],
        [
            StrBundleNode(
                LiteralNode("alice "),
                VarNode(LiteralNode("foo"), default=LiteralNode("loves")),
                LiteralNode(" bob"),
            ),
            [([], "alice $var(foo, default=loves) bob")],
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
            [(["key1"], 10), (["key2", "key1"], 10.2), (["key2", "key2"], "hello")],
        ],
        [
            ListNode(LiteralNode(10), LiteralNode(-0.25), ListNode(LiteralNode("aa"))),
            [([0], 10), ([1], -0.25), ([2, 0], "aa")],
        ],
    ],
)
def test_walk(node: Node, expected: Any):
    assert not DeepDiff(walk(node), expected)
