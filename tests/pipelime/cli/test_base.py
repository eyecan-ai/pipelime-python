import os
import traceback
from typing import Any, List

import pytest
import yaml
from pydantic import Field

from pipelime.piper import PipelimeCommand


class SimpleCommand(PipelimeCommand, title="simple-command"):
    simple_list: List[Any] = Field(..., title="Simple list")

    def run(self) -> None:
        pass


class TestCliBase:
    def _base_launch(self, args: list, exit_code=0, exc=None, user_input: str = ""):
        from typer.testing import CliRunner

        from pipelime.cli.main import _create_typer_app

        runner = CliRunner()
        result = runner.invoke(_create_typer_app(), args, input=user_input)

        print("*** CLI COMMAND OUTPUT ***", result.output, sep="\n")
        if result.stderr_bytes is not None:
            print("*** CLI COMMAND STDERR ***", result.stderr, sep="\n")
        if result.exc_info is not None:
            print("*** CLI COMMAND EXCEPTION ***")
            traceback.print_exception(
                result.exc_info[0],
                value=result.exc_info[1],
                tb=result.exc_info[2],
            )
        print("EXIT CODE:", result.exit_code)

        assert result.exit_code == exit_code
        assert (
            result.exception is None
            if exc is None
            else isinstance(result.exception, exc)
        )
        return result

    def test_help(self, extra_modules):
        for module_data in extra_modules:
            for cmd in module_data["operators"] + module_data["commands"]:
                result = self._base_launch(
                    ["-m", str(module_data["filepath"]), "help", cmd]
                )
                assert cmd in result.output

    def test_list(self):
        result = self._base_launch(["list"])
        assert "Pipelime Commands" in result.output
        assert "Sample Stages" in result.output
        assert "Sequence Generators" in result.output
        assert "Sequence Piped Operations" in result.output

        # NB: the names are different
        result = self._base_launch(["--verbose", "list"])
        assert "Pipelime Command" in result.output
        assert "Sample Stage" in result.output
        assert "Sequence Generator" in result.output
        assert "Sequence Piped Operation" in result.output
        assert "Fields" in result.output
        assert "Description" in result.output
        assert "Type" in result.output
        assert "Default" in result.output

    def test_list_extra(self, extra_modules):
        def _check(result, *, in_modules=[], not_in_modules=[]):
            for v in in_modules:
                assert v in result.output
            for v in not_in_modules:
                assert v not in result.output

        args = [
            v
            for module_data in extra_modules
            for v in ("-m", str(module_data["filepath"]))
        ] + ["list"]

        result = self._base_launch(args)
        for module_data in extra_modules:
            _check(
                result, in_modules=module_data["operators"] + module_data["commands"]
            )

        result = self._base_launch(["--verbose"] + args)
        for module_data in extra_modules:
            _check(
                result, in_modules=module_data["operators"] + module_data["commands"]
            )
        assert "Fields" in result.output
        assert "Description" in result.output
        assert "Type" in result.output
        assert "Default" in result.output

    @pytest.mark.parametrize("verbose", ["", "-v", "-vv", "-vvv", "-vvvv"])
    def test_run(self, minimnist_dataset, verbose, tmp_path):
        from pathlib import Path
        from typing import Sequence

        import numpy as np

        from pipelime.items import NpyNumpyItem
        from pipelime.sequences import SamplesSequence

        outpath = tmp_path / "output"
        args = [
            "pipe",
            "+input.folder",
            str(minimnist_dataset["path"]),
            "+output.folder",
            str(outpath),
            "+output.serialization.override.DEEP_COPY",
            "NpyNumpyItem",
            "+operations.slice.stop",
            "10",
        ]

        if verbose:
            args.append(verbose)

        self._base_launch(args)

        outreader = SamplesSequence.from_underfolder(outpath)  # type: ignore
        gt = SamplesSequence.from_underfolder(
            minimnist_dataset["path"]
        ).slice(  # type: ignore
            stop=10
        )
        assert len(outreader) == len(gt)
        for o, g in zip(outreader, gt):
            assert o.keys() == g.keys()
            for k, v in o.items():
                if isinstance(v(), np.ndarray):
                    assert np.array_equal(v(), g[k](), equal_nan=True)  # type: ignore
                else:
                    assert v() == g[k]()

                v_file_sources = v.local_sources

                assert isinstance(v_file_sources, Sequence)
                assert len(v_file_sources) == 1
                path = Path(v_file_sources[0])
                assert not path.is_symlink()
                assert path.is_file()
                assert (
                    path.stat().st_nlink == 1
                    if isinstance(v, NpyNumpyItem)
                    else path.stat().st_nlink > 1
                )

    @pytest.mark.parametrize(
        ["cmd_line", "parsed_cfg", "parsed_ctx", "should_fail"],
        [
            [
                ["+arg1", "++arg2", "@arg1", "@@arg2"],
                {"arg1": True, "arg2": True},
                {"arg1": True, "arg2": True},
                False,
            ],
            [
                [
                    "@arg1.arg2",
                    "41.3",
                    '+arg1.arg2="42.3"',
                    "++arg1.arg2",
                    "43",
                    "@@arg1.arg3=44",
                ],
                {"arg1": {"arg2": ["42.3", 43]}},
                {"arg1": {"arg2": 41.3, "arg3": 44}},
                False,
            ],
            [
                [
                    "+arg1.arg2",
                    "42",
                    "++arg1.arg3",
                    "'43'",
                    "//",
                    "+arg3.arg4",
                    "strval",
                    "++arg3.arg4",
                    "45",
                ],
                {"arg1": {"arg2": 42, "arg3": "43"}},
                {"arg3": {"arg4": ["strval", 45]}},
                False,
            ],
            [["+arg1[2]", "42"], {"arg1": [None, None, 42]}, {}, False],
            [
                [
                    "+arg1",
                    "+arg2",
                    "true",
                    "+arg3",
                    "True",
                    "+arg4",
                    "TRUE",
                    "+arg5",
                    "TrUe",
                    "+arg6",
                    "tRuE",
                ],
                {f"arg{i+1}": True for i in range(6)},
                {},
                False,
            ],
            [
                [
                    "+arg1",
                    "false",
                    "+arg2",
                    "False",
                    "+arg3",
                    "FALSE",
                    "+arg4",
                    "FaLsE",
                    "+arg5",
                    "fAlSe",
                ],
                {f"arg{i+1}": False for i in range(5)},
                {},
                False,
            ],
            [
                [
                    "+arg1",
                    "none",
                    "+arg2",
                    "NoNe",
                    "+arg3",
                    "null",
                    "+arg4",
                    "nUlL",
                    "+arg5",
                    "nul",
                    "+arg6",
                    "NuL",
                ],
                {f"arg{i+1}": None for i in range(6)},
                {},
                False,
            ],
            [
                [
                    "@arg1.arg2",
                    "41.3",
                    "name",
                    '+arg1.arg2="42.3"',
                    '"new name"',
                    "++arg1.arg2",
                    "43",
                    "@@arg1.arg3=44",
                ],
                {"arg1": {"arg2": ["42.3", "new name", 43]}},
                {"arg1": {"arg2": [41.3, "name"], "arg3": 44}},
                False,
            ],
            [
                [
                    "+arg1.arg2",
                    "42",
                    "name",
                    "++arg1.arg3",
                    "'43'",
                    "'new name'",
                    "//",
                    "+arg3.arg4",
                    "strval",
                    "name",
                    "++arg3.arg4",
                    "45",
                    "'new name'",
                ],
                {"arg1": {"arg2": [42, "name"], "arg3": ["43", "new name"]}},
                {"arg3": {"arg4": ["strval", "name", 45, "new name"]}},
                False,
            ],
            [["+arg1[2]", "42", "43"], {"arg1": [None, None, [42, 43]]}, {}, False],
            [["+arg1", "+arg1.arg2", "42", "++arg1.arg2", "43"], None, None, True],
            [["@arg.", "42"], None, None, True],
            [["+arg.[2]", "42"], None, None, True],
            [["+arg1..arg2", "42"], None, None, True],
            [["--arg"], None, None, True],
            [["45", "+arg"], None, None, True],
            [["name", "@arg"], None, None, True],
        ],
    )
    def test_cli_parser(self, cmd_line, parsed_cfg, parsed_ctx, should_fail):
        from pipelime.cli.parser import CLIParsingError, parse_pipelime_cli

        try:
            cmdline_cfg, cmdline_ctx = parse_pipelime_cli(cmd_line)
        except CLIParsingError:
            assert should_fail
        else:
            assert not should_fail
            assert cmdline_cfg == parsed_cfg
            assert cmdline_ctx == parsed_ctx

    def test_command_outputs(self, minimnist_dataset, tmp_path):
        import json

        out1 = tmp_path / "out1"
        out2 = tmp_path / "out2"
        cmdout = tmp_path / "cmdout.json"
        args = [
            "split",
            "+i",
            str(minimnist_dataset["path"]),
            "+s",
            f"0.2,{out1}",
            "+s",
            f"0.5,{out2}",
            "--command-outputs",
            str(cmdout),
        ]

        self._base_launch(args)

        assert cmdout.exists() and cmdout.is_file()
        with open(cmdout, "r") as f:
            data = json.load(f)
        assert isinstance(data, dict)
        assert "splits" in data
        assert data["splits"] == [
            out1.resolve().absolute().as_posix(),
            out2.resolve().absolute().as_posix(),
        ]

    @pytest.mark.parametrize("with_default_ckpt", [False, True, 2])
    def test_resume(self, ckpt_dag, minimnist_dataset, tmp_path, with_default_ckpt):
        from pydantic import ValidationError

        from pipelime.sequences import SamplesSequence

        if with_default_ckpt is False:
            ckpt_args = ["--checkpoint", str(tmp_path / "ckpt")]
            ckpt_resume = ["+ckpt", str(tmp_path / "ckpt")]
        else:
            ckpt_args = []
            ckpt_resume = (
                [] if with_default_ckpt is True else ["+ckpt", str(with_default_ckpt)]
            )

        outpath = tmp_path / "final_output"
        args = [
            "-m",
            ckpt_dag,
            "cat-and-split",
            "+properties.do_shuffle",
            "+properties.slices",
            "30",
            "+properties.main_data",
            str(minimnist_dataset["path"]),
            "+properties.datalist[0]",
            str(minimnist_dataset["path"]),
            "+properties.output",
            str(outpath),
        ] + ckpt_args

        # the first time this DAG will stop
        self._base_launch(args, exit_code=1, exc=RuntimeError)
        assert not outpath.exists()

        if not isinstance(with_default_ckpt, bool):
            # create fake command calls to fill the other default checkpoints
            for _ in range(with_default_ckpt - 1):
                self._base_launch(["clone"], exit_code=1, exc=TypeError)

        # now resume from checkpoint and override the slice size (invalid value)
        self._base_launch(
            ["resume", "+properties.slices", "-1"] + ckpt_resume,
            exit_code=1,
            exc=ValidationError,
        )
        assert not outpath.exists()

        # now resume again from checkpoint and override the slice size (valid value)
        self._base_launch(["resume", "+properties.slices", "5"] + ckpt_resume)
        assert outpath.is_dir()
        assert len(SamplesSequence.from_underfolder(outpath)) == 5

    def test_resume_with_tui(self, minimnist_dataset, tmp_path, monkeypatch):
        from pydantic import ValidationError
        from textual.keys import Keys
        from textual.pilot import Pilot

        from pipelime.cli.tui import TuiApp
        from pipelime.cli.tui.tui import Constants
        from pipelime.sequences import SamplesSequence

        tui_run = TuiApp.run

        def tui_mock_fail(app: TuiApp, *, headless=False, size=None, auto_pilot=None):
            async def autopilot(pilot: Pilot):
                # wait for the tui to be ready
                await pilot.pause()
                # insert value in field "input"
                for c in str(minimnist_dataset["path"]):
                    await pilot.press(c)
                # move to next field
                await pilot.press(Keys.Tab)
                # insert wrong value in field "output" (i.e., the same as input)
                for c in str(minimnist_dataset["path"]):
                    await pilot.press(c)
                # exit from the tui
                await pilot.press(Constants.TUI_KEY_CONFIRM)

            return tui_run(app, headless=True, auto_pilot=autopilot)

        monkeypatch.setattr(TuiApp, "run", tui_mock_fail)

        # the clone command should fail because of the wrong output path
        self._base_launch(
            ["clone", "--checkpoint", str(tmp_path / "ckpt")],
            exit_code=1,
            exc=ValidationError,
        )

        outpath = tmp_path / "clone_output"

        def tui_mock_pass(app: TuiApp, *, headless=False, size=None, auto_pilot=None):
            async def autopilot(pilot: Pilot):
                # wait for the tui to be ready
                await pilot.pause()
                # move to field "output"
                await pilot.press(Keys.Tab)
                # delete previously inserted value
                for _ in str(minimnist_dataset["path"]):
                    await pilot.press(Keys.Backspace)
                # insert correct value in field "output"
                for c in str(outpath):
                    await pilot.press(c)
                # exit from the tui
                await pilot.press(Constants.TUI_KEY_CONFIRM)

            return tui_run(app, headless=True, auto_pilot=autopilot)

        monkeypatch.setattr(TuiApp, "run", tui_mock_pass)

        # now the clone command should pass
        self._base_launch(["resume", "+ckpt", str(tmp_path / "ckpt")])

        assert outpath.is_dir()
        ss = SamplesSequence.from_underfolder(outpath)
        assert len(ss) == minimnist_dataset["len"]

    def test_missing_var_in_cfg(self, tmp_path) -> None:
        cfg = {"simple_list": [1, "a", "$var(third_element)"]}
        cfg_path = tmp_path / "cfg.yaml"
        with open(cfg_path, "w") as f:
            yaml.dump(cfg, f)

        out_cfg_file = tmp_path / "out_cfg.yaml"

        args = [
            "-m",
            f"{os.path.realpath(__file__)}",
            "simple-command",
            "-c",
            str(cfg_path),
            "-o",
            str(out_cfg_file),
        ]
        user_input = "16.12345\n"

        self._base_launch(args, user_input=user_input)

        with open(out_cfg_file, "r") as f:
            out_cfg = yaml.safe_load(f)

        assert list(out_cfg.keys()) == ["simple_list"]
        assert out_cfg["simple_list"] == [1, "a", 16.12345]

    def test_missing_var_in_ctx(self, tmp_path) -> None:
        cfg = {"simple_list": [1, "a", "$var(third_element)"]}
        cfg_path = tmp_path / "cfg.yaml"
        with open(cfg_path, "w") as f:
            yaml.dump(cfg, f)

        ctx = {"third_element": "$var(not_defined)"}
        ctx_path = tmp_path / "ctx.yaml"
        with open(ctx_path, "w") as f:
            yaml.dump(ctx, f)

        out_cfg_file = tmp_path / "out_cfg.yaml"

        args = [
            "-m",
            f"{os.path.realpath(__file__)}",
            "simple-command",
            "-c",
            str(cfg_path),
            "-x",
            str(ctx_path),
            "-o",
            str(out_cfg_file),
        ]
        user_input = "{'value': ['hello', 1.23, [7, 8, 9]]}\n"

        self._base_launch(args, user_input=user_input)

        with open(out_cfg_file, "r") as f:
            out_cfg = yaml.safe_load(f)

        assert list(out_cfg.keys()) == ["simple_list"]
        assert out_cfg["simple_list"] == [1, "a", {"value": ["hello", 1.23, [7, 8, 9]]}]

    def test_missing_for_in_cfg(self, tmp_path) -> None:
        cfg = {"simple_list": {"$for(elements, x)": ["Element number $item(x.number)"]}}
        cfg_path = tmp_path / "cfg.yaml"
        with open(cfg_path, "w") as f:
            yaml.dump(cfg, f)

        out_cfg_file = tmp_path / "out_cfg.yaml"

        args = [
            "-m",
            f"{os.path.realpath(__file__)}",
            "simple-command",
            "-c",
            str(cfg_path),
            "-o",
            str(out_cfg_file),
        ]
        user_input = "[{number: 10}, {number: 100}, {number: 1000}]\n"

        self._base_launch(args, user_input=user_input)

        with open(out_cfg_file, "r") as f:
            out_cfg = yaml.safe_load(f)

        assert list(out_cfg.keys()) == ["simple_list"]
        assert out_cfg["simple_list"] == [
            "Element number 10",
            "Element number 100",
            "Element number 1000",
        ]

    def test_missing_for_in_ctx(self, tmp_path) -> None:
        cfg = {"simple_list": "$var(defined_in_ctx)"}
        cfg_path = tmp_path / "cfg.yaml"
        with open(cfg_path, "w") as f:
            yaml.dump(cfg, f)

        ctx = {
            "defined_in_ctx": {
                "$for(X, x)": {"$for(Y, y)": ["Element number $item(x),$item(y)"]}
            }
        }
        ctx_path = tmp_path / "ctx.yaml"
        with open(ctx_path, "w") as f:
            yaml.dump(ctx, f)

        out_cfg_file = tmp_path / "out_cfg.yaml"

        args = [
            "-m",
            f"{os.path.realpath(__file__)}",
            "simple-command",
            "-c",
            str(cfg_path),
            "-x",
            str(ctx_path),
            "-o",
            str(out_cfg_file),
        ]
        user_input = "3\n4\n"

        self._base_launch(args, user_input=user_input)

        with open(out_cfg_file, "r") as f:
            out_cfg = yaml.safe_load(f)

        assert list(out_cfg.keys()) == ["simple_list"]
        expected_list = [f"Element number {x},{y}" for x in range(3) for y in range(4)]
        assert out_cfg["simple_list"] == expected_list

    def test_missing_switch_in_cfg(self, tmp_path) -> None:
        cfg = {
            "simple_list": {
                "$switch(option)": [
                    {"$case": "a", "$then": [1, 2, 3]},
                    {"$case": "b", "$then": ["a", "b", "c"]},
                    {"$default": [1.2, 3.4, 5.6]},
                ]
            }
        }
        cfg_path = tmp_path / "cfg.yaml"
        with open(cfg_path, "w") as f:
            yaml.dump(cfg, f)

        out_cfg_file = tmp_path / "out_cfg.yaml"

        args = [
            "-m",
            f"{os.path.realpath(__file__)}",
            "simple-command",
            "-c",
            str(cfg_path),
            "-o",
            str(out_cfg_file),
        ]

        inputs = ["a\n", "b\n", "c\n"]
        expected = [[1, 2, 3], ["a", "b", "c"], [1.2, 3.4, 5.6]]

        for i, e in zip(inputs, expected):
            self._base_launch(args, user_input=i)

            with open(out_cfg_file, "r") as f:
                out_cfg = yaml.safe_load(f)

            assert list(out_cfg.keys()) == ["simple_list"]
            assert out_cfg["simple_list"] == e

    def test_missing_switch_in_ctx(self, tmp_path) -> None:
        cfg = {
            "simple_list": "$var(defined_in_ctx)",
        }
        cfg_path = tmp_path / "cfg.yaml"
        with open(cfg_path, "w") as f:
            yaml.dump(cfg, f)

        ctx = {
            "defined_in_ctx": {
                "$switch(option)": [
                    {"$case": "op1", "$then": ["l", "i", "s", "t"]},
                    {"$case": "op2", "$then": [{"v": 1}, {"v": 2}, {"v": 3}]},
                    {"$default": [None, None, None]},
                ]
            }
        }
        ctx_path = tmp_path / "ctx.yaml"
        with open(ctx_path, "w") as f:
            yaml.dump(ctx, f)

        out_cfg_file = tmp_path / "out_cfg.yaml"

        args = [
            "-m",
            f"{os.path.realpath(__file__)}",
            "simple-command",
            "-c",
            str(cfg_path),
            "-x",
            str(ctx_path),
            "-o",
            str(out_cfg_file),
        ]

        inputs = ["op1\n", "op2\n", "xyz\n"]
        expected = [
            ["l", "i", "s", "t"],
            [{"v": 1}, {"v": 2}, {"v": 3}],
            [None, None, None],
        ]

        for i, e in zip(inputs, expected):
            self._base_launch(args, user_input=i)

            with open(out_cfg_file, "r") as f:
                out_cfg = yaml.safe_load(f)

            assert list(out_cfg.keys()) == ["simple_list"]
            assert out_cfg["simple_list"] == e

    def test_missing_vars_in_cfg_and_ctx(self, tmp_path) -> None:
        cfg = {"simple_list": ["$var(first_element)", "$var(second_element)"]}
        cfg_path = tmp_path / "cfg.yaml"
        with open(cfg_path, "w") as f:
            yaml.dump(cfg, f)

        ctx = {"first_element": "$var(not_defined)"}
        ctx_path = tmp_path / "ctx.yaml"
        with open(ctx_path, "w") as f:
            yaml.dump(ctx, f)

        out_cfg_file = tmp_path / "out_cfg.yaml"

        args = [
            "-m",
            f"{os.path.realpath(__file__)}",
            "simple-command",
            "-c",
            str(cfg_path),
            "-x",
            str(ctx_path),
            "-o",
            str(out_cfg_file),
        ]
        user_input = "123\nthe_second_element\n"

        self._base_launch(args, user_input=user_input)

        with open(out_cfg_file, "r") as f:
            out_cfg = yaml.safe_load(f)

        assert list(out_cfg.keys()) == ["simple_list"]
        assert out_cfg["simple_list"] == [123, "the_second_element"]

    def test_missing_var_in_cfg_no_ui(self, tmp_path) -> None:
        cfg = {"simple_list": "$var(not_defined)"}
        cfg_path = tmp_path / "cfg.yaml"
        with open(cfg_path, "w") as f:
            yaml.dump(cfg, f)

        args = [
            "-m",
            f"{os.path.realpath(__file__)}",
            "simple-command",
            "-c",
            str(cfg_path),
            "--no-ui",
        ]

        result = self._base_launch(args, exit_code=1, exc=SystemExit)

        assert (
            "ERROR: Invalid configuration! Variable not found: `not_defined`"
            in result.output
        )

    def test_missing_var_in_ctx_no_ui(self, tmp_path) -> None:
        cfg = {"simple_list": [1, 2, 3]}
        cfg_path = tmp_path / "cfg.yaml"
        with open(cfg_path, "w") as f:
            yaml.dump(cfg, f)

        ctx = {"cool_name": "$var(gimme_a_cool_value)"}
        ctx_path = tmp_path / "ctx.yaml"
        with open(ctx_path, "w") as f:
            yaml.dump(ctx, f)

        args = [
            "-m",
            f"{os.path.realpath(__file__)}",
            "simple-command",
            "-c",
            str(cfg_path),
            "-x",
            str(ctx_path),
            "--no-ui",
        ]

        result = self._base_launch(args, exit_code=1, exc=SystemExit)

        assert (
            "ERROR: Invalid context! Variable not found: `gimme_a_cool_value`"
            in result.output
        )
