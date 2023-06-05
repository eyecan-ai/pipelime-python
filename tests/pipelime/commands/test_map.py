import pytest
from .test_general_base import TestGeneralCommandsBase

from ... import TestUtils


class TestMap(TestGeneralCommandsBase):
    @pytest.mark.parametrize("nproc", [0, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_map(self, minimnist_dataset, nproc, prefetch, tmp_path):
        from pipelime.commands import MapCommand
        from pipelime.sequences import SamplesSequence

        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "output": (tmp_path / "output").as_posix(),
            "grabber": f"{nproc},{prefetch}",
            "stage": {"filter-keys": {"key_list": minimnist_dataset["image_keys"]}},
        }
        cmd = MapCommand.parse_obj(params)
        cmd()

        # check output
        outseq = SamplesSequence.from_underfolder(params["output"])
        srcseq = SamplesSequence.from_underfolder(params["input"])
        for o, s in zip(outseq, srcseq):
            assert set(o.keys()) == set(minimnist_dataset["image_keys"])
            for k, v in o.items():
                assert TestUtils.numpy_eq(v(), s[k]())
