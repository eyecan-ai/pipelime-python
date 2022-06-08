class TestCliBase:
    def _base_launch(self, args: list):
        from typer.testing import CliRunner

        from pipelime.cli.main import app

        runner = CliRunner()
        result = runner.invoke(app, args)
        print(result.output)
        assert result.exit_code == 0
        assert result.exception is None
        return result

    def test_help(self, extra_modules):
        for module_data in extra_modules:
            for cmd in module_data["operators"] + module_data["commands"]:
                result = self._base_launch(
                    ["-m", str(module_data["filepath"]), "help", cmd]
                )
                assert cmd in result.output

    def test_list(self, extra_modules):
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
        assert "Required" in result.output

    def test_run(self, data_folder, minimnist_dataset, tmp_path):
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
            "+operations",
            r"{slice: {stop: 10}, '"
            + str(data_folder / "cli" / "extra_operators.py")
            + r":reversed': {num: 5} }",
        ]

        self._base_launch(args)

        outreader = SamplesSequence.from_underfolder(outpath)  # type: ignore
        gt = (
            SamplesSequence.from_underfolder(minimnist_dataset["path"])  # type: ignore
            .slice(stop=10)
            .reverse(num=5)
        )
        assert len(outreader) == len(gt)
        for o, g in zip(outreader, gt):
            assert o.keys() == g.keys()
            for k, v in o.items():
                if isinstance(v(), np.ndarray):
                    assert np.array_equal(v(), g[k](), equal_nan=True)
                else:
                    assert v() == g[k]()
                assert isinstance(v._file_sources, Sequence)
                assert len(v._file_sources) == 1
                path = Path(v._file_sources[0])
                assert not path.is_symlink()
                assert path.is_file()
                assert (
                    path.stat().st_nlink == 1
                    if isinstance(v, NpyNumpyItem)
                    else path.stat().st_nlink > 1
                )
