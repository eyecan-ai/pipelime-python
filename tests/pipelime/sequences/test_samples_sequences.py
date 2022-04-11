import pytest
import pipelime.sequences as pls
import typing as t


class TestSamplesSequences:
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
                assert root_sample[k].is_shared == True
                if merge_root_items:
                    assert k in sample
                    assert sample[k] is root_sample[k]
                else:
                    assert k not in sample
            for k in minimnist_dataset["item_keys"]:
                assert k in sample
                assert sample[k].is_shared == False

    def test_from_list(self, minimnist_dataset: dict):
        source = pls.SamplesSequence.from_underfolder(  # type: ignore
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        sseq = pls.SamplesSequence.from_list(source.samples)  # type: ignore

        assert len(source) == len(sseq)
        assert source.samples is sseq.samples

        for src_smpl, sample in zip(source, sseq):
            assert src_smpl is sample
