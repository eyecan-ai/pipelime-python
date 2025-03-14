import os
from pathlib import Path, PurePosixPath
from typing import Tuple

import numpy as np
import pytest
from deepdiff import DeepDiff
from pydantic.v1 import BaseModel

import pipelime.choixe.visitors.processor as processor_module
from pipelime.choixe.ast.parser import parse
from pipelime.choixe.utils.io import load
from pipelime.choixe.visitors import process


@pytest.fixture()
def mock_rand(monkeypatch):
    class MockRand:
        def __init__(self) -> None:
            self.invoked = []

        def __call__(self, *args, **kwargs) -> float:
            self.invoked.append((args, kwargs))
            return 0.5

    mock_rand = MockRand()
    monkeypatch.setattr(processor_module, "rand", mock_rand)
    return mock_rand


class MyCompositeClass:
    def __init__(self, a, b):
        self.a = a
        self.b = b

    def __eq__(self, __o: object) -> bool:
        return __o.a == self.a and __o.b == self.b  # type: ignore


class MyModel2(BaseModel):
    a: int
    b: str


class MyModel(BaseModel):
    a: int
    b: float
    c: Tuple[int, float]
    d: Tuple[MyModel2, MyModel2]


class TestProcessor:
    context = {
        "color": {"hue": "red"},
        "animal": "cow",
        "collection1": list(range(1000, 1010)),
        "collection2": [str(x) for x in range(100, 105)],
        "collection3": list(range(50, 52)),
        "collection4": [str(x) for x in range(100, 102)],
        "nested_stuff": {
            "a": 10,
            "b": {
                "sweep": "$sweep(10, 20, 30)",
                "c": 20,
            },
        },
    }
    env = {"VAR1": "yellow", "VAR2": "snake"}

    def _expectation_test(self, data, expected, allow_branching: bool = True) -> None:
        for k, v in self.env.items():
            os.environ[k] = v

        try:
            parsed = parse(data)
            res = process(parsed, context=self.context, allow_branching=allow_branching)
            [print(x) for x in res]
            assert not DeepDiff(res, expected)

        finally:
            for k in self.env:
                del os.environ[k]

    def test_var_plain(self):
        data = "$var(color.hue, default='blue')"
        expected = ["red"]
        self._expectation_test(data, expected)

    def test_var_none_default(self):
        data = "$var(color.sat, default=None)"
        expected = [None]
        self._expectation_test(data, expected)

    def test_var_missing(self):
        data = "$var(color.sat, default='low')"
        expected = ["low"]
        self._expectation_test(data, expected)

    def test_var_str_bundle(self):
        data = {"a": "I am a $var(color.hue) $var(animal)"}
        expected = [{"a": "I am a red cow"}]
        self._expectation_test(data, expected)

    def test_env_plain(self):
        data = "$var(VAR1, default='blue', env=True)"
        expected = ["yellow"]
        self._expectation_test(data, expected)

    def test_env_missing(self):
        data = "$var(color.sat, default=25, env=True)"
        expected = [25]
        self._expectation_test(data, expected)

    def test_env_str_bundle(self):
        data = {"a": "I am a $var(VAR1, env=True) $var(VAR2, env=True)"}
        expected = [{"a": "I am a yellow snake"}]
        self._expectation_test(data, expected)

    def test_val_non_string(self):
        data = {
            "a": "$var(color.hue)",
            1: "$var(color.hue)",
            0.5: "$var(color.hue)",
        }
        expected = [{"a": "red", 1: "red", 0.5: "red"}]
        self._expectation_test(data, expected)

    def test_import_plain(self, choixe_plain_cfg: Path):
        path_str = str(PurePosixPath(choixe_plain_cfg)).replace(
            "\\", "/"
        )  # Windows please...
        data = {"a": f'$import("{path_str}")'}
        expected = [{"a": load(choixe_plain_cfg)}]
        self._expectation_test(data, expected)

    def test_import_relative(self, choixe_plain_cfg: Path):
        prev_cwd = os.getcwd()
        os.chdir(choixe_plain_cfg.parent)
        try:
            data = {"a": f'$import("{choixe_plain_cfg.name}")'}
            expected = [{"a": load(choixe_plain_cfg)}]
            self._expectation_test(data, expected)
        finally:
            os.chdir(prev_cwd)

    def test_sweep_base(self):
        data = {
            "a": {
                "$directive": "sweep",
                "$args": [1096, 20.0, "40", "$var(color.hue)"],
                "$kwargs": {},
            },
            "b": {
                "a": "$sweep('hello')",
                "b": "$sweep('hello', 'world')",
                "c": "$sweep('hello', 'world')",
                "d": 10,
            },
        }
        expected = [
            {"a": 1096, "b": {"a": "hello", "b": "hello", "c": "hello", "d": 10}},
            {"a": 20.0, "b": {"a": "hello", "b": "hello", "c": "hello", "d": 10}},
            {"a": "40", "b": {"a": "hello", "b": "hello", "c": "hello", "d": 10}},
            {"a": "red", "b": {"a": "hello", "b": "hello", "c": "hello", "d": 10}},
            {"a": 1096, "b": {"a": "hello", "b": "world", "c": "hello", "d": 10}},
            {"a": 20.0, "b": {"a": "hello", "b": "world", "c": "hello", "d": 10}},
            {"a": "40", "b": {"a": "hello", "b": "world", "c": "hello", "d": 10}},
            {"a": "red", "b": {"a": "hello", "b": "world", "c": "hello", "d": 10}},
            {"a": 1096, "b": {"a": "hello", "b": "hello", "c": "world", "d": 10}},
            {"a": 20.0, "b": {"a": "hello", "b": "hello", "c": "world", "d": 10}},
            {"a": "40", "b": {"a": "hello", "b": "hello", "c": "world", "d": 10}},
            {"a": "red", "b": {"a": "hello", "b": "hello", "c": "world", "d": 10}},
            {"a": 1096, "b": {"a": "hello", "b": "world", "c": "world", "d": 10}},
            {"a": 20.0, "b": {"a": "hello", "b": "world", "c": "world", "d": 10}},
            {"a": "40", "b": {"a": "hello", "b": "world", "c": "world", "d": 10}},
            {"a": "red", "b": {"a": "hello", "b": "world", "c": "world", "d": 10}},
        ]
        self._expectation_test(data, expected)

    def test_sweep_in_context(self):
        data = {"a": "$var(nested_stuff.b.sweep)"}
        expected = [{"a": 10}, {"a": 20}, {"a": 30}]
        self._expectation_test(data, expected)

    def test_sweep_no_branching(self):
        data = {
            "a": {
                "$directive": "sweep",
                "$args": [1096, 20.0, "40", "$var(color.hue)"],
                "$kwargs": {},
            },
            "b": {
                "a": "$sweep(hello)",
                "b": "$sweep(hello, world)",
                "c": "$sweep(hello, world)",
                "d": 10,
            },
        }
        self._expectation_test(data, [data], allow_branching=False)

    def test_sweep_lists(self):
        data = ["$sweep(10, 20)", {"a": [10, "$sweep(30, 40)"]}]
        expected = [
            [10, {"a": [10, 30]}],
            [20, {"a": [10, 30]}],
            [10, {"a": [10, 40]}],
            [20, {"a": [10, 40]}],
        ]
        self._expectation_test(data, expected)

    def test_sweep_str_bundle(self):
        data = {"a": "I am a $sweep('red', 'blue') $sweep('sheep', 'cow')"}
        expected = [
            {"a": "I am a red sheep"},
            {"a": "I am a blue sheep"},
            {"a": "I am a red cow"},
            {"a": "I am a blue cow"},
        ]
        self._expectation_test(data, expected)

    def test_sweep_multikey(self):
        data = {
            "$sweep('foo', 'bar')": "$sweep('alice', 'bob')",
            "$sweep('alpha', 'beta')": "$sweep(10, 20)",
        }
        expected = [
            {"foo": "alice", "alpha": 10},
            {"foo": "bob", "alpha": 10},
            {"bar": "alice", "alpha": 10},
            {"bar": "bob", "alpha": 10},
            {"foo": "alice", "alpha": 20},
            {"foo": "bob", "alpha": 20},
            {"bar": "alice", "alpha": 20},
            {"bar": "bob", "alpha": 20},
            {"foo": "alice", "beta": 10},
            {"foo": "bob", "beta": 10},
            {"bar": "alice", "beta": 10},
            {"bar": "bob", "beta": 10},
            {"foo": "alice", "beta": 20},
            {"foo": "bob", "beta": 20},
            {"bar": "alice", "beta": 20},
            {"bar": "bob", "beta": 20},
        ]
        self._expectation_test(data, expected)

    def test_sweep_nested(self):
        data = {
            "foo": {
                "$directive": "sweep",
                "$args": [{"$sweep(foo, bar)": "10"}, {"foo": "$sweep(20, 30)"}],
                "$kwargs": {},
            }
        }
        expected = [
            {"foo": {"foo": "10"}},
            {"foo": {"bar": "10"}},
            {"foo": {"foo": 20}},
            {"foo": {"foo": 30}},
        ]
        self._expectation_test(data, expected)

    def test_symbol(self):
        data = {
            "a": "$symbol(numpy.zeros)",
            "b": "$symbol(builtins.float)",
        }
        expected = [
            {"a": np.zeros, "b": float},
        ]
        self._expectation_test(data, expected)

    def test_instance(self):
        data = {
            "$call": f"{__file__}:MyCompositeClass",
            "$args": {
                "a": {
                    "$call": f"{__file__}:MyCompositeClass",
                    "$args": {
                        "a": 10,
                        "b": 20,
                    },
                },
                "b": {
                    "$call": f"{__file__}:MyCompositeClass",
                    "$args": {
                        "a": {
                            "$call": f"{__file__}:MyCompositeClass",
                            "$args": {
                                "a": 30,
                                "b": 40,
                            },
                        },
                        "b": {
                            "$call": f"{__file__}:MyCompositeClass",
                            "$args": {
                                "a": 50,
                                "b": 60,
                            },
                        },
                    },
                },
            },
        }
        expected = [
            MyCompositeClass(
                MyCompositeClass(10, 20),
                MyCompositeClass(MyCompositeClass(30, 40), MyCompositeClass(50, 60)),
            )
        ]
        assert process(parse(data)) == expected

    def test_model(self):
        data = {
            "$model": f"{__file__}:MyModel",
            "$args": {
                "a": 10,
                "b": 0.1,
                "c": [20, 0.32],
                "d": [{"a": 98, "b": "hello"}, {"a": 24, "b": "world"}],
            },
        }
        expected = [
            MyModel(
                a=10,
                b=0.1,
                c=(20, 0.32),
                d=(MyModel2(a=98, b="hello"), MyModel2(a=24, b="world")),
            )
        ]
        assert process(parse(data)) == expected

    def test_for_dict(self):
        data = {"$for(collection1, x)": {"Index=$index(x)": "Item=$item(x)"}}
        expected = [
            {
                f"Index={i}": f"Item={x}"
                for i, x in enumerate(self.context["collection1"])
            }
        ]
        self._expectation_test(data, expected)

    def test_for_list(self):
        data = {"$for(collection2, x)": ["$index(x)->$item(x)", 10]}
        expected = [[]]
        [
            expected[0].extend([f"{i}->{x}", 10])
            for i, x in enumerate(self.context["collection2"])
        ]
        self._expectation_test(data, expected)

    def test_for_str(self):
        data = {"$for(collection2, x)": "$index(x)->$item(x)"}
        expected = [
            "".join([f"{i}->{x}" for i, x in enumerate(self.context["collection2"])])
        ]
        self._expectation_test(data, expected)

    def test_for_dict_compact(self):
        data = {"$for(collection1)": {"Index=$index": "Item=$item"}}
        expected = [
            {
                f"Index={i}": f"Item={x}"
                for i, x in enumerate(self.context["collection1"])
            }
        ]
        self._expectation_test(data, expected)

    def test_for_nested(self):
        data = {
            "$for(collection3, x)": {
                "item_$index(x)=$item(x)": {
                    "$for(collection4, y)": ["item_$index(x)_$index(y)=$item(y)"]
                }
            }
        }
        expected = [
            {
                "item_0=50": ["item_0_0=100", "item_0_1=101"],
                "item_1=51": ["item_1_0=100", "item_1_1=101"],
            }
        ]
        self._expectation_test(data, expected)

    def test_for_direct_nesting(self):
        data = {
            "$for(collection3, x)": {
                "$for(collection4, y)": ["item_$index(x)_$index(y)=$item(y)"]
            }
        }
        expected = [
            ["item_0_0=100", "item_0_1=101", "item_1_0=100", "item_1_1=101"],
        ]
        self._expectation_test(data, expected)

    def test_for_int_element_dict(self):
        data = {"$for(3)": {"$index": "$index"}}
        expected = [{0: 0, 1: 1, 2: 2}]
        self._expectation_test(data, expected)

    def test_for_int_element_list(self):
        data = {"$for(3)": ["$index"]}
        expected = [[0, 1, 2]]
        self._expectation_test(data, expected)

    def test_for_int_element_str(self):
        data = {"$for(3)": "$index"}
        expected = ["012"]
        self._expectation_test(data, expected)

    def test_for_nested_compact(self):
        data = {
            "$for(collection3)": {
                "item_$index = $item": {"$for(collection4)": ["item_$index = $item"]}
            }
        }
        expected = [
            {
                "item_0 = 50": ["item_0 = 100", "item_1 = 101"],
                "item_1 = 51": ["item_0 = 100", "item_1 = 101"],
            }
        ]
        self._expectation_test(data, expected)

    def test_for_sweep(self):
        data = {"$for(collection3, x)": {"$index(x)": "$item(x)=$sweep('a', 'b')"}}
        expected = [
            {0: "50=a", 1: "51=a"},
            {0: "50=a", 1: "51=b"},
            {0: "50=b", 1: "51=a"},
            {0: "50=b", 1: "51=b"},
        ]
        self._expectation_test(data, expected)

    def test_for_multiple(self):
        data = {
            "$for(collection3, x)": {"a$index": "$item"},
            "$for(collection2, y)": {"b$index": "$item"},
            "$for(collection3, z)": {"c$index": "$item"},
            "a": 10,
            "b": [0, 2, 3],
        }
        expected = [
            {
                "a0": 50,
                "a1": 51,
                "b0": "100",
                "b1": "101",
                "b2": "102",
                "b3": "103",
                "b4": "104",
                "c0": 50,
                "c1": 51,
                "a": 10,
                "b": [0, 2, 3],
            }
        ]
        self._expectation_test(data, expected)

    def test_switch_base(self):
        data = {
            "$switch(animal)": [
                {"$case": ["cat", "dog", "hamster"], "$then": 10},
                {"$case": ["cow"], "$then": 20},
            ]
        }
        expected = [20]
        self._expectation_test(data, expected)

    def test_switch_default(self):
        data = {
            "$switch(animal)": [
                {"$case": ["cat", "dog", "hamster"], "$then": 10},
                {"$case": ["horse"], "$then": 20},
                {"$default": 30},
            ]
        }
        expected = [30]
        self._expectation_test(data, expected)

    def test_switch_no_match(self):
        data = {
            "$switch(animal)": [
                {"$case": ["seal"], "$then": 200},
            ]
        }
        with pytest.raises(processor_module.ChoixeProcessingError):
            process(parse(data))

    def test_switch_not_found(self):
        data = {
            "$switch(WRONG)": [
                {"$case": ["value"], "$then": 200},
            ]
        }
        with pytest.raises(processor_module.ChoixeProcessingError):
            process(parse(data))

    def test_uuid(self):
        data = "$uuid"
        processed = process(parse(data))
        assert len(processed) == 1 and isinstance(processed[0], str)

    def test_date(self):
        data = "$date"
        processed = process(parse(data))
        assert len(processed) == 1 and isinstance(processed[0], str)

    def test_date_format(self):
        data = "$date('%Y')"
        processed = process(parse(data))
        assert len(processed) == 1 and isinstance(processed[0], str)

    def test_cmd(self):
        data = "$cmd('python --version')"
        processed = process(parse(data))
        assert len(processed) == 1 and isinstance(processed[0], str)

    def test_tmp(self):
        data = "$tmp"
        processed = process(parse(data))
        assert len(processed) == 1 and isinstance(processed[0], str)

    def test_tmp_name(self):
        data = "$tmp(my_tmp)"
        processed = process(parse(data))
        assert len(processed) == 1 and isinstance(processed[0], str)

    def test_rand(self, mock_rand):
        data = "$rand()"
        processed = process(parse(data))
        assert len(processed) == 1
        assert len(mock_rand.invoked[0][0]) == 0
        assert len(mock_rand.invoked[0][1]) == 0

    def test_rand_with_args(self, mock_rand):
        data = "$rand(5, 10)"
        processed = process(parse(data))
        assert len(processed) == 1
        assert tuple(mock_rand.invoked[0][0]) == (5, 10)
        assert len(mock_rand.invoked[0][1]) == 0

    def test_rand_with_kwargs(self, mock_rand):
        data = "$rand(n=5, pdf=[0.1, 0.2, 0.3, 0.4])"
        processed = process(parse(data))
        assert len(processed) == 1
        assert len(mock_rand.invoked[0][0]) == 0
        assert mock_rand.invoked[0][1] == {"n": 5, "pdf": [0.1, 0.2, 0.3, 0.4]}

    def test_rand_with_args_and_kwargs(self, mock_rand):
        data = "$rand(5, 10, n=5, pdf=[0.1, 0.2, 0.3, 0.4])"
        processed = process(parse(data))
        assert len(processed) == 1
        assert tuple(mock_rand.invoked[0][0]) == (5, 10)
        assert mock_rand.invoked[0][1] == {"n": 5, "pdf": [0.1, 0.2, 0.3, 0.4]}

    def test_for_mindfuck(self):
        data = {
            "$for(collection3, x)": [
                "$sweep(1, 2)$item(x)",
                {"$for(collection4)": ["$sweep(3, 4)$index(x)$item"]},
            ]
        }
        expected = [
            ["150", ["30100", "30101"], "151", ["31100", "31101"]],
            ["150", ["30100", "30101"], "251", ["31100", "31101"]],
            ["150", ["30100", "30101"], "151", ["31100", "41101"]],
            ["150", ["30100", "30101"], "251", ["31100", "41101"]],
            ["150", ["30100", "30101"], "151", ["41100", "31101"]],
            ["150", ["30100", "30101"], "251", ["41100", "31101"]],
            ["150", ["30100", "30101"], "151", ["41100", "41101"]],
            ["150", ["30100", "30101"], "251", ["41100", "41101"]],
            ["250", ["30100", "30101"], "151", ["31100", "31101"]],
            ["250", ["30100", "30101"], "251", ["31100", "31101"]],
            ["250", ["30100", "30101"], "151", ["31100", "41101"]],
            ["250", ["30100", "30101"], "251", ["31100", "41101"]],
            ["250", ["30100", "30101"], "151", ["41100", "31101"]],
            ["250", ["30100", "30101"], "251", ["41100", "31101"]],
            ["250", ["30100", "30101"], "151", ["41100", "41101"]],
            ["250", ["30100", "30101"], "251", ["41100", "41101"]],
            ["150", ["30100", "40101"], "151", ["31100", "31101"]],
            ["150", ["30100", "40101"], "251", ["31100", "31101"]],
            ["150", ["30100", "40101"], "151", ["31100", "41101"]],
            ["150", ["30100", "40101"], "251", ["31100", "41101"]],
            ["150", ["30100", "40101"], "151", ["41100", "31101"]],
            ["150", ["30100", "40101"], "251", ["41100", "31101"]],
            ["150", ["30100", "40101"], "151", ["41100", "41101"]],
            ["150", ["30100", "40101"], "251", ["41100", "41101"]],
            ["250", ["30100", "40101"], "151", ["31100", "31101"]],
            ["250", ["30100", "40101"], "251", ["31100", "31101"]],
            ["250", ["30100", "40101"], "151", ["31100", "41101"]],
            ["250", ["30100", "40101"], "251", ["31100", "41101"]],
            ["250", ["30100", "40101"], "151", ["41100", "31101"]],
            ["250", ["30100", "40101"], "251", ["41100", "31101"]],
            ["250", ["30100", "40101"], "151", ["41100", "41101"]],
            ["250", ["30100", "40101"], "251", ["41100", "41101"]],
            ["150", ["40100", "30101"], "151", ["31100", "31101"]],
            ["150", ["40100", "30101"], "251", ["31100", "31101"]],
            ["150", ["40100", "30101"], "151", ["31100", "41101"]],
            ["150", ["40100", "30101"], "251", ["31100", "41101"]],
            ["150", ["40100", "30101"], "151", ["41100", "31101"]],
            ["150", ["40100", "30101"], "251", ["41100", "31101"]],
            ["150", ["40100", "30101"], "151", ["41100", "41101"]],
            ["150", ["40100", "30101"], "251", ["41100", "41101"]],
            ["250", ["40100", "30101"], "151", ["31100", "31101"]],
            ["250", ["40100", "30101"], "251", ["31100", "31101"]],
            ["250", ["40100", "30101"], "151", ["31100", "41101"]],
            ["250", ["40100", "30101"], "251", ["31100", "41101"]],
            ["250", ["40100", "30101"], "151", ["41100", "31101"]],
            ["250", ["40100", "30101"], "251", ["41100", "31101"]],
            ["250", ["40100", "30101"], "151", ["41100", "41101"]],
            ["250", ["40100", "30101"], "251", ["41100", "41101"]],
            ["150", ["40100", "40101"], "151", ["31100", "31101"]],
            ["150", ["40100", "40101"], "251", ["31100", "31101"]],
            ["150", ["40100", "40101"], "151", ["31100", "41101"]],
            ["150", ["40100", "40101"], "251", ["31100", "41101"]],
            ["150", ["40100", "40101"], "151", ["41100", "31101"]],
            ["150", ["40100", "40101"], "251", ["41100", "31101"]],
            ["150", ["40100", "40101"], "151", ["41100", "41101"]],
            ["150", ["40100", "40101"], "251", ["41100", "41101"]],
            ["250", ["40100", "40101"], "151", ["31100", "31101"]],
            ["250", ["40100", "40101"], "251", ["31100", "31101"]],
            ["250", ["40100", "40101"], "151", ["31100", "41101"]],
            ["250", ["40100", "40101"], "251", ["31100", "41101"]],
            ["250", ["40100", "40101"], "151", ["41100", "31101"]],
            ["250", ["40100", "40101"], "251", ["41100", "31101"]],
            ["250", ["40100", "40101"], "151", ["41100", "41101"]],
            ["250", ["40100", "40101"], "251", ["41100", "41101"]],
        ]
        self._expectation_test(data, expected)
