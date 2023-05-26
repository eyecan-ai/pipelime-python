import pytest
from pydantic import ValidationError
from .test_general_base import TestGeneralCommandsBase

from ... import TestAssert


class TestPipe(TestGeneralCommandsBase):
    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_pipe(self, minimnist_dataset, nproc, prefetch, tmp_path):
        from pipelime.commands import PipeCommand
        from pipelime.sequences import SamplesSequence
        from pipelime.stages import StageKeysFilter

        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "output": (tmp_path / "output").as_posix(),
            "operations": {
                "map": {
                    "stage": StageKeysFilter(  # type: ignore
                        key_list=minimnist_dataset["image_keys"]
                    )
                }
            },
            "grabber": {
                "num_workers": nproc,
                "prefetch": prefetch,
            },
        }

        cmd = PipeCommand.parse_obj(params)
        cmd()

        src = SamplesSequence.from_underfolder(minimnist_dataset["path"])
        dst = SamplesSequence.from_underfolder(tmp_path / "output")
        assert len(src) == len(dst) == minimnist_dataset["len"]
        for s1, s2 in zip(src, dst):
            TestAssert.samples_equal(
                s1.extract_keys(*minimnist_dataset["image_keys"]), s2
            )

        params["operations"] = {}
        with pytest.raises(ValidationError):
            cmd = PipeCommand.parse_obj(params)
