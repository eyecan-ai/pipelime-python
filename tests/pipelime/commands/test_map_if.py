from pathlib import Path

import pytest

from ... import TestUtils
from .test_general_base import TestGeneralCommandsBase


class TestMapIf(TestGeneralCommandsBase):
    @pytest.mark.parametrize("nproc", [0, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_map_if(self, minimnist_dataset, nproc, prefetch, tmp_path):
        from pipelime.commands import MapIfCommand
        from pipelime.sequences import SamplesSequence
        from pipelime.items import NumpyItem

        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "output": (tmp_path / "output").as_posix(),
            "grabber": f"{nproc},{prefetch}",
            "stage": {"filter-keys": {"key_list": minimnist_dataset["image_keys"]}},
            "condition": f"{Path(__file__).with_name('helper.py')}:map_if_fn",
        }
        cmd = MapIfCommand.parse_obj(params)
        cmd()

        # check output
        outseq = SamplesSequence.from_underfolder(params["output"])
        srcseq = SamplesSequence.from_underfolder(params["input"])
        for idx, (o, s) in enumerate(zip(outseq, srcseq)):
            if idx % 2 == 0 or idx == 3:
                assert set(o.keys()) == set(
                    minimnist_dataset["image_keys"] + minimnist_dataset["root_keys"]
                )
            else:
                assert set(o.keys()) == set(s.keys())
            for k, v in o.items():
                sv = s[k]
                assert v.__class__ == sv.__class__
                if isinstance(v, NumpyItem):
                    assert TestUtils.numpy_eq(v(), sv())
                else:
                    assert v() == sv()
