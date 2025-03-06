from pathlib import Path

import pytest
from pydantic import ValidationError, parse_obj_as

from ... import TestAssert
from .test_general_base import TestGeneralCommandsBase


class TestSplit(TestGeneralCommandsBase):
    @pytest.mark.parametrize("fraction", [0.1, 0.8, 1.0, None])
    @pytest.mark.parametrize("folder", [None, "outf"])
    def test_perc_split(self, fraction, folder):
        from pipelime.commands.split_ops import PercSplit

        def _parse_and_check(data):
            parsed = parse_obj_as(PercSplit, data)
            if fraction is not None:
                assert parsed.fraction == fraction
                assert parsed.split_size(100) == fraction * 100
            else:
                assert parsed.fraction is None

            if folder is not None:
                assert parsed.output is not None
                assert parsed.output.folder == Path(folder).absolute()
            else:
                assert parsed.output is None

        _parse_and_check(PercSplit(fraction=fraction, output=folder))
        _parse_and_check(
            {
                "fraction": fraction,
                **({} if folder is None else {"output": folder}),
            }
        )

        if folder is None and fraction is not None:
            _parse_and_check(fraction)

        folder_str = "" if folder is None else f",{folder}"
        if fraction is None:
            _parse_and_check("nOnE" + folder_str)
            _parse_and_check("NulL" + folder_str)
            _parse_and_check("nUl" + folder_str)
        else:
            _parse_and_check(str(fraction) + folder_str)

    def test_invalid_perc_split(self):
        from pipelime.commands.split_ops import PercSplit

        with pytest.raises(ValidationError):
            parse_obj_as(PercSplit, 0.0)

        with pytest.raises(ValidationError):
            parse_obj_as(PercSplit, 2)

    @pytest.mark.parametrize("length", [1, 8, 10, None])
    @pytest.mark.parametrize("folder", [None, "outf"])
    def test_abs_split(self, length, folder):
        from pipelime.commands.split_ops import AbsoluteSplit

        def _parse_and_check(data):
            parsed = parse_obj_as(AbsoluteSplit, data)
            if length is not None:
                assert parsed.length == length
                assert parsed.split_size(100) == length
            else:
                assert parsed.length is None

            if folder is not None:
                assert parsed.output is not None
                assert parsed.output.folder == Path(folder).absolute()
            else:
                assert parsed.output is None

        _parse_and_check(AbsoluteSplit(length=length, output=folder))
        _parse_and_check(
            {
                "length": length,
                **({} if folder is None else {"output": folder}),
            }
        )

        if folder is None and length is not None:
            _parse_and_check(length)

        folder_str = "" if folder is None else f",{folder}"
        if length is None:
            _parse_and_check("nOnE" + folder_str)
            _parse_and_check("NulL" + folder_str)
            _parse_and_check("nUl" + folder_str)
        else:
            _parse_and_check(str(length) + folder_str)

    def test_invalid_abs_split(self):
        from pipelime.commands.split_ops import AbsoluteSplit

        with pytest.raises(ValidationError):
            parse_obj_as(AbsoluteSplit, 0)

        with pytest.raises(ValidationError):
            parse_obj_as(AbsoluteSplit, 2.3)

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

    @pytest.mark.parametrize("output_selected", [None, "out_selected"])
    @pytest.mark.parametrize("output_discarded", [None, "out_discarded"])
    @pytest.mark.parametrize("nproc", [0, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_split_query(
        self,
        minimnist_dataset,
        output_selected,
        output_discarded,
        nproc,
        prefetch,
        tmp_path,
    ):
        from pipelime.commands import SplitByQueryCommand
        from pipelime.sequences import SamplesSequence

        if output_selected is not None:
            output_selected = tmp_path / output_selected

        if output_discarded is not None:
            output_discarded = tmp_path / output_discarded

        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "query": "`metadata.sample_id` < 12",
            "output_selected": output_selected,
            "output_discarded": output_discarded,
            "grabber": f"{nproc},{prefetch}",
        }
        cmd = SplitByQueryCommand.parse_obj(params)
        cmd()

        inseq = SamplesSequence.from_underfolder(params["input"])

        if output_selected is not None:
            outseq_selected = SamplesSequence.from_underfolder(output_selected)
            TestAssert.sequences_equal(inseq[:12], outseq_selected)

        if output_discarded is not None:
            outseq_discarded = SamplesSequence.from_underfolder(output_discarded)
            TestAssert.sequences_equal(inseq[12:], outseq_discarded)

    @pytest.mark.parametrize(("key", "nsplits"), [("label", 10), ("values.data", 4)])
    @pytest.mark.parametrize("nproc", [0, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_split_value(
        self,
        minimnist_dataset,
        key,
        nsplits,
        nproc,
        prefetch,
        tmp_path,
    ):
        from pipelime.commands import SplitByValueCommand
        from pipelime.sequences import SamplesSequence

        base_out = tmp_path / "outvals"
        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "key": key,
            "output": base_out,
            "grabber": f"{nproc},{prefetch}",
        }
        cmd = SplitByValueCommand.parse_obj(params)
        cmd()

        subpaths = list(Path(base_out).glob("*"))
        assert len(subpaths) == nsplits

        inseq = SamplesSequence.from_underfolder(params["input"])
        split_size = len(inseq) // nsplits
        for i in range(nsplits):
            name = base_out / f"{key}={i}"
            if name not in subpaths:
                name = base_out / f"{key}={i}.0"  # for float values
                assert name in subpaths

            outseq = SamplesSequence.from_underfolder(name)
            TestAssert.sequences_equal(
                inseq[split_size * i : split_size * (i + 1)], outseq
            )
