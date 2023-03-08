import pytest
import pipelime.sequences as pls
import typing as t
from pathlib import Path


class TestSamplesSequencesSources:
    @pytest.mark.parametrize("merge_root_items", [True, False])
    def test_from_underfolder(self, minimnist_dataset: dict, merge_root_items):
        sseq = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=merge_root_items
        )

        assert isinstance(sseq, pls.SamplesSequence)
        assert minimnist_dataset["len"] == len(sseq)
        assert isinstance(sseq[0], pls.Sample)

        root_sample = sseq.root_sample  # type: ignore
        assert isinstance(root_sample, pls.Sample)

        sample_list = sseq._samples  # type: ignore
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

    @pytest.mark.parametrize(
        "root_folder", [Path("no-path"), Path(__file__).parent.resolve()]
    )
    def test_from_underfolder_must_exist(self, root_folder: Path):
        sseq = pls.SamplesSequence.from_underfolder(
            folder=root_folder, must_exist=False
        )
        assert sseq.folder == root_folder  # type: ignore
        assert not sseq.must_exist  # type: ignore

        with pytest.raises(ValueError):
            sseq = pls.SamplesSequence.from_underfolder(
                folder=root_folder, must_exist=True
            )

    def test_from_list(self, minimnist_dataset: dict):
        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        sseq = pls.SamplesSequence.from_list([s for s in source])  # type: ignore

        assert len(source) == len(sseq)
        assert all(s1 is s2 for s1, s2 in zip(source, sseq))

    def test_from_callable(self, minimnist_dataset: dict):
        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        )

        def _gen_fn(index: int) -> pls.Sample:
            return source[index]

        def _len_fn() -> int:
            return len(source)

        sseq = pls.SamplesSequence.from_callable(generator_fn=_gen_fn, length=_len_fn)

        assert len(source) == len(sseq)
        assert all(s1 is s2 for s1, s2 in zip(source, sseq))

        sseq = pls.SamplesSequence.from_callable(
            generator_fn=_gen_fn, length=len(source)
        )

        assert len(source) == len(sseq)
        assert all(s1 is s2 for s1, s2 in zip(source, sseq))
