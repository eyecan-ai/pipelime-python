import pytest


class TestCliBase:
    def _base_launch(self, args: list, exit_code=0, exc=None):
        from typer.testing import CliRunner

        from pipelime.cli.main import _create_typer_app

        runner = CliRunner()
        result = runner.invoke(_create_typer_app(), args)

        print("*** CLI COMMAND OUTPUT ***", result.output, sep="\n")
        print("EXCEPTION:", type(result.exception))
        print(result.exception)
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

    def test_run(self, minimnist_dataset, tmp_path):
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

    def test_resume(self, ckpt_dag, minimnist_dataset, tmp_path):
        from pipelime.sequences import SamplesSequence
        from pydantic import ValidationError

        outpath = tmp_path / "final_output"
        args = [
            "-m",
            ckpt_dag,
            "--checkpoint",
            str(tmp_path / "ckpt"),
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
        ]

        # the first time this DAG will stop
        self._base_launch(args, exit_code=1, exc=RuntimeError)
        assert not outpath.exists()

        # now resume from checkpoint and override the slice size (invalid value)
        self._base_launch(
            ["resume", "+ckpt", str(tmp_path / "ckpt"), "+properties.slices", "-1"],
            exit_code=1,
            exc=ValidationError,
        )
        assert not outpath.exists()

        # now resume from checkpoint and override the slice size (valid value)
        self._base_launch(
            ["resume", "+ckpt", str(tmp_path / "ckpt"), "+properties.slices", "5"]
        )
        assert outpath.is_dir()
        assert len(SamplesSequence.from_underfolder(outpath)) == 5
