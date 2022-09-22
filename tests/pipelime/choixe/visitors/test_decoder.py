from pathlib import Path
from typing import Any

import numpy as np
import pytest
from pipelime.choixe.ast.nodes import DictNode, ListNode, Node, LiteralNode
from pipelime.choixe.visitors import decode
from deepdiff import DeepDiff
from pydantic import BaseModel


class Person(BaseModel):
    id_: str
    age: int


class Cat(BaseModel):
    age: int
    weight: float
    name: str
    owner: Person


@pytest.mark.parametrize(
    ["node", "expected"],
    [
        [
            DictNode(
                nodes={LiteralNode(data="foo"): LiteralNode(data=np.zeros((2, 3, 2)))}
            ),
            {
                "foo": [
                    [[0.0, 0.0], [0.0, 0.0], [0.0, 0.0]],
                    [[0.0, 0.0], [0.0, 0.0], [0.0, 0.0]],
                ]
            },
        ],
        [
            ListNode(LiteralNode(data=np.zeros((2, 3, 2)))),
            [
                [
                    [[0.0, 0.0], [0.0, 0.0], [0.0, 0.0]],
                    [[0.0, 0.0], [0.0, 0.0], [0.0, 0.0]],
                ]
            ],
        ],
        [LiteralNode(data=np.uint8(24)), 24],
        [LiteralNode(data=np.float64(0.125)), 0.125],
        [
            LiteralNode(
                data=Cat(
                    age=10,
                    weight=5.23,
                    name="Oliver",
                    owner=Person(id_="OCJ123j", age=32),
                )
            ),
            {
                "$model": "tests.pipelime.choixe.visitors.test_decoder.Cat",
                "$args": {
                    "age": 10,
                    "weight": 5.23,
                    "name": "Oliver",
                    "owner": {
                        "id_": "OCJ123j",
                        "age": 32,
                    },
                },
            },
        ],
        [LiteralNode(data=Path("/tmp/foo.txt")), str(Path("/tmp/foo.txt"))],
    ],
)
def test_decode(node: Node, expected: Any):
    assert not DeepDiff(decode(node), expected)
