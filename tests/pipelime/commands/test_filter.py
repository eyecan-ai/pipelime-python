from pathlib import Path

import pytest
from .test_general_base import TestGeneralCommandsBase


class TestFilter(TestGeneralCommandsBase):
    @pytest.mark.parametrize("nproc", [0, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_filter(self, minimnist_dataset, nproc, prefetch, tmp_path):
        from pipelime.commands import FilterCommand
        from pipelime.sequences import SamplesSequence

        def _check_output(path):
            outseq = SamplesSequence.from_underfolder(path)
            for x in outseq:
                assert x.deep_get("metadata.double") == 6

        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "output": (tmp_path / "output_key").as_posix(),
            "grabber": f"{nproc},{prefetch}",
            "filter_query": "`metadata.double` == 6",
        }
        cmd = FilterCommand.parse_obj(params)
        cmd()
        _check_output(params["output"])

        params["output"] = (tmp_path / "output_fn").as_posix()
        params["filter_fn"] = f"{Path(__file__).with_name('helper.py')}:filter_fn"
        with pytest.raises(ValueError):
            cmd = FilterCommand.parse_obj(params)

        del params["filter_query"]
        if nproc == 0:
            cmd = FilterCommand.parse_obj(params)
            cmd()
            _check_output(params["output"])
            with pytest.raises(ValueError):
                cmd = FilterCommand.parse_obj(params)  # output exists
        else:
            del params["filter_fn"]
            with pytest.raises(ValueError):
                cmd = FilterCommand.parse_obj(params)
