import pytest
import pipelime.sequences as pls
from ... import TestUtils, TestAssert


class TestSamplesSequences:
    def test_name(self):
        from pipelime.sequences.pipes import PipedSequenceBase
        from pipelime.sequences.pipes.mapping import MappedSequence

        assert not PipedSequenceBase.__config__.title
        assert PipedSequenceBase.name() == PipedSequenceBase.__name__

        assert bool(MappedSequence.__config__.title)
        assert MappedSequence.name() == MappedSequence.__config__.title

    def test_is_normalized(self, minimnist_dataset: dict):
        import pipelime.items as pli

        sseq = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"]
        ).cat(
            pls.SamplesSequence.from_list(  # type: ignore
                [pls.Sample({"__": pli.UnknownItem()})]
            )
        )

        assert sseq.is_normalized(1)
        assert sseq.is_normalized(5)
        assert not sseq.is_normalized()

    @pytest.mark.parametrize(
        ("length", "zfill"),
        [
            (5, 1),
            (50, 2),
            (500, 3),
            (5000, 4),
            (10, 1),
            (100, 2),
            (1000, 3),
            (10000, 4),
        ],
    )
    def test_best_zfill(self, length: int, zfill: int):
        seq = pls.SamplesSequence.from_list([pls.Sample()] * length)
        assert seq.best_zfill() == zfill

    def test_to_pipe(self):
        from pipelime.stages import StageIdentity
        from pathlib import Path

        a = pls.SamplesSequence.from_underfolder(
            folder="no-path", must_exist=False
        ).slice(start=10)
        b = pls.SamplesSequence.from_underfolder(
            folder="no-path-2", must_exist=False
        ).shuffle()
        a = a.cat(b)

        expected_pipe = [
            {
                "from_underfolder": {
                    "folder": Path("no-path"),
                    "merge_root_items": True,
                    "must_exist": False,
                    "watch": False,
                }
            },
            {"slice": {"start": 10, "stop": None, "step": None}},
            {"cat": {"to_cat": b}},
        ]

        assert a.to_pipe(recursive=False, objs_to_str=False) == expected_pipe

        expected_pipe[2]["cat"]["to_cat"] = [
            {
                "from_underfolder": {
                    "folder": Path("no-path-2"),
                    "merge_root_items": True,
                    "must_exist": False,
                    "watch": False,
                }
            },
            {"shuffle": {"seed": None}},
        ]

        assert a.to_pipe(recursive=True, objs_to_str=False) == expected_pipe

    def test_build_pipe(self):
        from pydantic import ValidationError
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
                "map": {"identity": None},
            },
            {"data_cache": ["ImageItem", "MetadataItem"]},
        ]

        expected_seq = (
            pls.SamplesSequence.from_underfolder(
                folder="no-path", merge_root_items=True, must_exist=False
            )
            .slice(start=10)
            .map(StageIdentity())
            .data_cache("ImageItem", "MetadataItem")
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
            "data_cache": ["ImageItem", "MetadataItem"],
        }

        assert pls.build_pipe(input_pipe).dict() == expected_seq.dict()

        with pytest.raises(ValidationError):
            pls.build_pipe("shuffle")

        with pytest.raises(ValueError):
            pls.build_pipe(pipe_list=[])

    def _direct_access_check_results(self, sseq, da_seq):
        import numpy as np

        assert len(da_seq) == len(sseq)

        for s1, s2 in zip(sseq, da_seq):
            assert s1.keys() == s2.keys()
            for k, v1 in s1.items():
                v1 = v1()
                v2 = s2[k]
                assert v1.__class__ == v2.__class__
                if isinstance(v1, np.ndarray):
                    assert np.array_equal(v1, v2, equal_nan=True)
                else:
                    assert v1 == v2

    def test_direct_access(self, minimnist_dataset: dict):
        sseq = pls.SamplesSequence.from_underfolder(folder=minimnist_dataset["path"])
        da_seq = sseq.direct_access()
        self._direct_access_check_results(sseq, da_seq)

    def test_direct_access_slicing(self, minimnist_dataset: dict):
        sseq = pls.SamplesSequence.from_underfolder(folder=minimnist_dataset["path"])
        da_seq = sseq.direct_access()
        self._direct_access_check_results(sseq[2:12:3], da_seq[2:12:3])

    def test_direct_access_sum(self, minimnist_dataset: dict):
        sseq_0 = pls.SamplesSequence.from_underfolder(folder=minimnist_dataset["path"])
        sseq_1 = pls.SamplesSequence.toy_dataset(10)
        da_seq0 = sseq_0.direct_access()
        self._direct_access_check_results(
            sseq_0 + sseq_1, da_seq0 + sseq_1.direct_access()
        )
        self._direct_access_check_results(sseq_0 + sseq_1, da_seq0 + sseq_1)

    @pytest.mark.parametrize("drop_last", [True, False])
    def test_batch(self, minimnist_dataset: dict, drop_last: bool):
        sseq = pls.SamplesSequence.from_underfolder(folder=minimnist_dataset["path"])
        sseq = pls.SamplesSequence.from_list([s for s in sseq])

        batch_size = 6
        count = 0
        for batch in sseq.batch(batch_size=batch_size, drop_last=drop_last):
            idx_offs = count * batch_size
            last_valid_idx = min(idx_offs + batch_size, len(sseq))
            assert all(
                bsmpl is orig_smpl
                for bsmpl, orig_smpl in zip(
                    batch, sseq[idx_offs : idx_offs + last_valid_idx]
                )
            )

            effective_size = last_valid_idx - idx_offs
            assert not drop_last or effective_size == batch_size

            if effective_size != batch_size:
                assert all(len(s) == 0 for s in batch[effective_size:])
            count += 1

    def test_apply(self, minimnist_dataset: dict):
        from pipelime.stages import StageLambda

        sseq = (
            pls.SamplesSequence.from_underfolder(folder=minimnist_dataset["path"])
            .slice(start=10)
            .map(StageLambda(lambda x: x.extract_keys(minimnist_dataset["image_keys"])))
        )
        out_seq = sseq.apply()

        for s1, s2 in zip(sseq, out_seq):
            TestAssert.samples_equal(s1, s2)

    def test_run(self, minimnist_dataset: dict):
        from pipelime.stages import StageLambda

        sseq = (
            pls.SamplesSequence.from_underfolder(folder=minimnist_dataset["path"])
            .slice(start=10)
            .map(StageLambda(lambda x: x.extract_keys(minimnist_dataset["image_keys"])))
        )
        out_seq = sseq.run(
            sample_fn=lambda x, idx: TestAssert.samples_equal(x, sseq[idx])
        )

    @pytest.mark.skipif(not TestUtils.has_torch(), reason="PyTorch not installed")
    def test_torch_dataset(self, minimnist_dataset: dict):
        from torch.utils.data import Dataset

        seq = pls.SamplesSequence.from_underfolder(folder=minimnist_dataset["path"])
        torch_dataset = seq.torch_dataset()

        assert isinstance(torch_dataset, Dataset)
        assert len(torch_dataset) == len(seq)  # type: ignore

        for sample in torch_dataset:
            assert isinstance(sample, dict)

        self._direct_access_check_results(seq, torch_dataset)
