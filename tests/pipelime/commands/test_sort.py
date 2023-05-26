from pathlib import Path

import pytest
from .test_general_base import TestGeneralCommandsBase


class TestSort(TestGeneralCommandsBase):
    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_sort(self, minimnist_dataset, nproc, prefetch, tmp_path):
        from pipelime.commands import SortCommand
        from pipelime.sequences import SamplesSequence

        def _check_output(path, deep_key):
            last_random = 0.0
            outseq = SamplesSequence.from_underfolder(path)
            for x in outseq:
                assert x.deep_get(deep_key) > last_random
                last_random = x.deep_get(deep_key)

        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "output": (tmp_path / "output_key").as_posix(),
            "grabber": f"{nproc},{prefetch}",
            "sort_key": "metadata.random",
        }
        cmd = SortCommand.parse_obj(params)
        cmd()
        _check_output(params["output"], params["sort_key"])

        params["output"] = (tmp_path / "output_fn").as_posix()
        params["sort_fn"] = f"{Path(__file__).with_name('helper.py')}:sort_fn"
        cmd = SortCommand.parse_obj(params)
        with pytest.raises(ValueError):
            cmd()

        del params["sort_key"]
        if nproc == 0:
            cmd = SortCommand.parse_obj(params)
            cmd()
            _check_output(params["output"], "metadata.random")
            with pytest.raises(ValueError):
                cmd = SortCommand.parse_obj(params)  # output exists
        else:
            del params["sort_fn"]
            cmd = SortCommand.parse_obj(params)
            with pytest.raises(ValueError):
                cmd()
