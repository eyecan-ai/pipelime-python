import pytest
from .test_general_base import TestGeneralCommandsBase

from ... import TestAssert


class TestSlice(TestGeneralCommandsBase):
    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    @pytest.mark.parametrize(
        "indexes",
        [
            (12, None, None),
            (None, 12, None),
            (None, None, 3),
            (12, 15, None),
            (None, 18, 3),
            (12, None, 3),
            (12, 18, 3),
        ],
    )
    @pytest.mark.parametrize("shuffle", [42, False])
    def test_slice(
        self, minimnist_dataset, nproc, prefetch, indexes, shuffle, tmp_path
    ):
        self._slice_check(
            minimnist_dataset,
            nproc,
            prefetch,
            indexes,
            shuffle,
            tmp_path / "output_slice",
        )

        indexes = (indexes[1], indexes[0], indexes[2])
        indexes = tuple(None if i is None else -i for i in indexes)
        self._slice_check(
            minimnist_dataset,
            nproc,
            prefetch,
            indexes,
            shuffle,
            tmp_path / "output_slice_neg",
        )

    def _slice_check(
        self, minimnist_dataset, nproc, prefetch, indexes, shuffle, tmp_path
    ):
        import random

        from pipelime.commands import SliceCommand
        from pipelime.sequences import SamplesSequence

        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "output": tmp_path.as_posix(),
            "grabber": f"{nproc},{prefetch}",
            "slice": ":".join(["" if x is None else str(x) for x in indexes]),
            "shuffle": shuffle,
        }
        cmd = SliceCommand.parse_obj(params)
        cmd()

        inseq = SamplesSequence.from_underfolder(params["input"])
        outseq = SamplesSequence.from_underfolder(params["output"])

        idxs = list(range(len(inseq)))
        if shuffle:
            rnd = random.Random(shuffle)
            rnd.shuffle(idxs)
        idxs = idxs[slice(*indexes)]
        assert len(outseq) == len(idxs)

        for i, s in zip(idxs, outseq):
            TestAssert.samples_equal(inseq[i], s)
