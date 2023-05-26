import pytest
from .test_general_base import TestGeneralCommandsBase

from ... import TestAssert


class TestZip(TestGeneralCommandsBase):
    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_zip(self, minimnist_dataset, nproc, prefetch, tmp_path):
        from pipelime.commands import ZipCommand
        from pipelime.sequences import SamplesSequence
        from pipelime.stages import StageKeyFormat

        # create three sequences
        src1p, src2p, src3p = tmp_path / "seq1", tmp_path / "seq2", tmp_path / "seq3"
        seq = SamplesSequence.from_underfolder(minimnist_dataset["path"])
        slice_len = len(seq) // 3
        seq[:slice_len].to_underfolder(src1p).run()
        seq[slice_len : 2 * slice_len].to_underfolder(src2p).run()
        seq[2 * slice_len :].to_underfolder(src3p).run()

        params = {
            "inputs": [
                src3p.as_posix(),
                src2p.as_posix(),
                src1p.as_posix(),
            ],
            "output": (tmp_path / "output").as_posix(),
            "grabber": f"{nproc},{prefetch}",
            "key_format": ["s2_*", "_s1"],
        }
        cmd = ZipCommand.parse_obj(params)
        cmd()

        src1 = SamplesSequence.from_underfolder(src1p).map(
            StageKeyFormat(key_format="*_s1")  # type: ignore
        )
        src2 = SamplesSequence.from_underfolder(src2p).map(
            StageKeyFormat(key_format="s2_*")  # type: ignore
        )
        src3 = SamplesSequence.from_underfolder(src3p)
        dest = SamplesSequence.from_underfolder(tmp_path / "output")
        assert min(len(src1), len(src2), len(src3)) == len(dest)
        for s1, s2, s3, d in zip(src1, src2, src3, dest):
            TestAssert.samples_equal(s3 + s2 + s1, d)

        params["key_format"].append("s3_*")
        with pytest.raises(ValueError):
            cmd = ZipCommand.parse_obj(params)  # output exists
