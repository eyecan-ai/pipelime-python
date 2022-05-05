from pathlib import Path

import pytest
from pipelime.choixe.ast.parser import parse
from pipelime.choixe.visitors import Inspection, inspect


class TestInspector:
    @pytest.mark.parametrize(
        ["expr", "expected"],
        [
            [{"a": 10, "b": {"a": 20.0}}, Inspection(processed=True)],
            [
                {
                    "$var(variable.one)": 10,
                    "b": "$var(variable.two, default=10.2)",
                    "c": "$var(variable.three, env=True)",
                },
                Inspection(
                    variables={"variable": {"one": None, "two": 10.2, "three": None}},
                    environ={"variable.three": None},
                ),
            ],
            [
                [
                    "$var(variable.one)",
                    "$var(variable.two, default=10.2)",
                    "$var(variable.three, env=True)",
                ],
                Inspection(
                    variables={"variable": {"one": None, "two": 10.2, "three": None}},
                    environ={"variable.three": None},
                ),
            ],
            [
                "String with $var(variable.one) $var(variable.two, default=10.2) $var(variable.three, env=True)",
                Inspection(
                    variables={"variable": {"one": None, "two": 10.2, "three": None}},
                    environ={"variable.three": None},
                ),
            ],
            [
                {
                    "$directive": "sweep",
                    "$args": [10, "$var(x)", "$var(variable.x)"],
                    "$kwargs": {},
                },
                Inspection(variables={"x": None, "variable": {"x": None}}),
            ],
            [
                {"$call": "numpy.array", "$args": {"shape": [4, 3, 2]}},
                Inspection(symbols={"numpy.array"}),
            ],
            [
                {"$model": "path/to/my_file.py:MyModel", "$args": {"shape": [4, 3, 2]}},
                Inspection(symbols={"path/to/my_file.py:MyModel"}),
            ],
            [
                {"$for(var.x.y, x)": {"$index(x)": "$item(x)"}},
                Inspection(variables={"var": {"x": {"y": None}}}),
            ],
            [
                {
                    "$for(var.my_var, x)": {"$index(x)": "$item(x)"},
                    "$var(var.another_var)": 10,
                    "$for(var.my_var2, y)": {"$index(y)": "$var(var.another_var2)"},
                },
                Inspection(
                    variables={
                        "var": {
                            "my_var": None,
                            "my_var2": None,
                            "another_var": None,
                            "another_var2": None,
                        }
                    }
                ),
            ],
        ],
    )
    def test_inspector(self, expr, expected):
        assert inspect(parse(expr)) == expected

    def test_relative_import(self, choixe_plain_cfg):
        data = {
            "a": f"$import('{choixe_plain_cfg.name}')",
            "b": {"c": f"$import('{choixe_plain_cfg.name}')"},
        }
        expected = Inspection(imports={choixe_plain_cfg})
        assert inspect(parse(data), cwd=choixe_plain_cfg.parent) == expected

    def test_import_not_found(self):
        data = {"a": "$import('nonexisting.yml')"}
        expected = Inspection(imports={Path("nonexisting.yml").absolute()})
        with pytest.warns(UserWarning):
            assert inspect(parse(data)) == expected
