import pytest
from .test_general_base import TestGeneralCommandsBase

from ... import TestAssert


class TestSplit(TestGeneralCommandsBase):
    @pytest.mark.parametrize("shuffle", [False, True, 1])
    @pytest.mark.parametrize(
        "splits",
        [
            [0.1],
            [1.0],
            [1],
            [10],
            [0.2, 0.3, 0.5],
            [2, 3, 5],
            [0.2, 0.3, 0.3, None],
            [2, 3, 3, None],
            [0.2, None, 0.3, 0.3],
            [2, None, 3, 3],
            [1, 0.1],
            [None, 1, 0.1],
            [0.1, 1],
            [None, 0.1, 1],
        ],
    )
    @pytest.mark.parametrize("subsample", [1, 2])
    @pytest.mark.parametrize("nproc", [0, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_split(
        self, minimnist_dataset, shuffle, subsample, splits, nproc, prefetch, tmp_path
    ):
        from pipelime.commands import SplitCommand
        from pipelime.sequences import SamplesSequence

        outputs = [f"{s},{tmp_path / f'output{i}'}" for i, s in enumerate(splits)]
        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "shuffle": shuffle,
            "subsample": subsample,
            "splits": outputs,
            "grabber": f"{nproc},{prefetch}",
        }
        cmd = SplitCommand.parse_obj(params)
        cmd()

        inseq = SamplesSequence.from_underfolder(params["input"])
        outseqs = [
            SamplesSequence.from_underfolder(x.split(",")[1]) for x in params["splits"]
        ]

        total_len = len(inseq) // subsample
        actual_lens = [len(x) for x in outseqs]
        expected_lens = []
        none_idx = None
        for i, split in enumerate(splits):
            if split is None:
                none_idx = i
            elif isinstance(split, int):
                expected_lens.append(split)
            else:
                expected_lens.append(int(len(inseq) // subsample * split))
        if none_idx is not None:
            expected_lens.insert(none_idx, total_len - sum(expected_lens))

        cumulative_idx = 0
        for i in range(len(outputs)):
            expected_len = expected_lens[i]
            actual_len = actual_lens[i]
            assert expected_len == actual_len
            if not shuffle:
                for idx in range(actual_len):
                    in_sample = inseq[::subsample][cumulative_idx + idx]
                    out_sample = outseqs[i][idx]
                    TestAssert.samples_equal(in_sample, out_sample)
                cumulative_idx += actual_len
