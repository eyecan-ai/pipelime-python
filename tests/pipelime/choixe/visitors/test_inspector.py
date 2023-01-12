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
                "$symbol(my_package.my_symbol)",
                Inspection(symbols={"my_package.my_symbol"}),
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
                    "$switch(nation)": [
                        {
                            "$case": ["UK", "$var(a_nation)", "Australia"],
                            "$then": "hello",
                        },
                        {
                            "$case": "Italy",
                            "$then": "$var(italian_salute)",
                        },
                        {
                            "$default": "$var(default_salute)",
                        },
                    ]
                },
                Inspection(
                    variables={
                        "nation": None,
                        "a_nation": None,
                        "italian_salute": None,
                        "default_salute": None,
                    }
                ),
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
            [
                {
                    "a": "$var(x.sub_var)",
                    "b": "$var(x)",
                },
                Inspection(variables={"x": {"sub_var": None}}),
            ],
            [
                {
                    "foo": "$var(my_var_a)",
                    "$for(my_var_b, x)": {
                        "b_$index(x)": "$item(x.sub_var_a)",
                        "a_$index(x)": "$item(x)",
                    },
                    "$for(my_var_c, y)": {
                        "a_$index(y)": "$item(y.sub_var_a)",
                        "c_$index(y)": "$item(y.sub_var_b.sub_sub_var_a)",
                        "b_$index(y)": "$item(y.sub_var_b)",
                    },
                    "$for(my_var_c, z)": {
                        "b_$index(z)": "$item(z.sub_var_a.sub_sub_var_b)",
                        "c_$index(z)": "$item(z.sub_var_b)",
                        "a_$index(z)": "$item(z.sub_var_a.sub_sub_var_a)",
                    },
                },
                Inspection(
                    variables={
                        "my_var_a": None,
                        "my_var_b": {"sub_var_a": None},
                        "my_var_c": {
                            "sub_var_a": {"sub_sub_var_a": None, "sub_sub_var_b": None},
                            "sub_var_b": {"sub_sub_var_a": None},
                        },
                    }
                ),
            ],
            [
                {
                    "a": "$rand()",
                    "b": "$rand(10)",
                    "c": "$rand(10, 20)",
                    "d": "$rand(10, 20, n=2)",
                    "e": "$rand(10, 20, n=2, pdf=lambda x: x + 1)",
                },
                Inspection(),
            ],
        ],
    )
    def test_inspector(self, expr, expected):
        print(inspect(parse(expr)))
        print(expected)
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
