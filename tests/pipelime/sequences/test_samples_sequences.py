import pytest
import pipelime.sequences as pls
import typing as t


class TestSamplesSequences:
    def test_is_normalized(self, minimnist_dataset: dict):
        import pipelime.items as pli

        sseq = pls.SamplesSequence.from_underfolder(  # type: ignore
            folder=minimnist_dataset["path"]
        ).cat(
            pls.SamplesSequence.from_list(  # type: ignore
                [pls.Sample({"__": pli.UnknownItem()})]
            )
        )

        assert sseq.is_normalized(1)
        assert sseq.is_normalized(5)
        assert not sseq.is_normalized()

    @pytest.mark.parametrize("merge_root_items", [True, False])
    def test_from_underfolder(self, minimnist_dataset: dict, merge_root_items):
        sseq = pls.SamplesSequence.from_underfolder(  # type: ignore
            folder=minimnist_dataset["path"], merge_root_items=merge_root_items
        )

        assert isinstance(sseq, pls.SamplesSequence)
        assert minimnist_dataset["len"] == len(sseq)

        root_sample = sseq.root_sample  # type: ignore
        assert isinstance(root_sample, pls.Sample)

        sample_list = sseq.samples  # type: ignore
        assert isinstance(sample_list, t.Sequence)
        assert minimnist_dataset["len"] == len(sample_list)
        assert isinstance(sample_list[0], pls.Sample)

        for sample, raw_sample in zip(sseq, sample_list):
            for k, v in raw_sample.items():
                assert sample[k] is v
            for k in minimnist_dataset["root_keys"]:
                assert k in root_sample
                assert root_sample[k].is_shared
                if merge_root_items:
                    assert k in sample
                    assert sample[k] is root_sample[k]
                else:
                    assert k not in sample
            for k in minimnist_dataset["item_keys"]:
                assert k in sample
                assert not sample[k].is_shared

    def test_from_underfolder_must_exist(self):
        sseq = pls.SamplesSequence.from_underfolder(  # type: ignore
            folder="no-path", must_exist=False
        )
        assert str(sseq.folder) == "no-path"
        assert not sseq.must_exist

        with pytest.raises(ValueError):
            sseq = pls.SamplesSequence.from_underfolder(  # type: ignore
                folder="no-path", must_exist=True
            )

    def test_from_list(self, minimnist_dataset: dict):
        source = pls.SamplesSequence.from_underfolder(  # type: ignore
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        sseq = pls.SamplesSequence.from_list(source.samples)  # type: ignore

        assert len(source) == len(sseq)
        assert source.samples == sseq.samples

    def test_to_pipe(self):
        from pipelime.stages import StageIdentity
        from pathlib import Path

        a = (
            pls.SamplesSequence.from_underfolder(  # type: ignore
                folder="no-path", must_exist=False
            )
            .map(StageIdentity())
            .slice(start=10)
        )
        b = pls.SamplesSequence.from_underfolder(  # type: ignore
            folder="no-path-2", must_exist=False
        ).shuffle()
        a = a.cat(b)

        expected_pipe = [
            {
                "from_underfolder": {
                    "folder": Path("no-path"),
                    "merge_root_items": True,
                    "must_exist": False,
                }
            },
            {"map": {"stage": StageIdentity()}},
            {"slice": {"start": 10, "stop": None, "step": None}},
            {"cat": {"to_cat": b}},
        ]

        assert a.to_pipe(recursive=False, objs_to_str=False) == expected_pipe

        expected_pipe[3]["cat"]["to_cat"] = [
            {
                "from_underfolder": {
                    "folder": Path("no-path-2"),
                    "merge_root_items": True,
                    "must_exist": False,
                }
            },
            {"shuffle": {"seed": None}},
        ]

        assert a.to_pipe(recursive=True, objs_to_str=False) == expected_pipe

    def test_build_pipe(self):
        from pipelime.stages import StageIdentity

        input_pipe = [
            {
                "from_underfolder": {
                    "folder": "no-path",
                    "merge_root_items": True,
                    "must_exist": False,
                }
            },
            {
                "slice": {"start": 10, "stop": None, "step": None},
                "map": {"stage": StageIdentity()},
            },
        ]

        expected_seq = (
            pls.SamplesSequence.from_underfolder(  # type: ignore
                folder="no-path", merge_root_items=True, must_exist=False
            )
            .slice(start=10)
            .map(StageIdentity())
        )

        assert pls.build_pipe(input_pipe).dict() == expected_seq.dict()

        input_pipe = {
            "from_underfolder": {
                "folder": "no-path",
                "merge_root_items": True,
                "must_exist": False,
            },
            "slice": {"start": 10, "stop": None, "step": None},
            "map": StageIdentity(),
        }

        assert pls.build_pipe(input_pipe).dict() == expected_seq.dict()

        with pytest.raises(AttributeError):
            pls.build_pipe("shuffle")
