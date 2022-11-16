import pytest
import typing as t
from pathlib import Path

import pipelime.sequences as pls


class TestSamplesSequenceOperations:
    def test_base(self):
        from pipelime.sequences import SamplesSequence, Sample
        from pipelime.sequences.pipes import PipedSequenceBase

        src = SamplesSequence.from_list([Sample({}) for _ in range(10)])
        seq = PipedSequenceBase(source=src)
        assert len(seq) == len(src)
        assert all(s1 is s2 for s1, s2 in zip(seq, src))

    def test_map(self, minimnist_dataset: dict):
        import pipelime.items as pli
        import pipelime.stages as plst

        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        ).map(
            plst.StageLambda(
                lambda x: pls.Sample(
                    {k: pli.JsonMetadataItem({"the_answer": 42}) for k in x}
                )
            )
        )
        for sample in source:
            assert all(k in sample for k in minimnist_dataset["item_keys"])
            for k, v in sample.items():
                assert k in minimnist_dataset["item_keys"]
                assert isinstance(v, pli.JsonMetadataItem)

                raw = v()
                assert isinstance(raw, t.Mapping)
                assert "the_answer" in raw
                assert raw["the_answer"] == 42

    @pytest.mark.parametrize("key_format", ["prefix*", "suffix"])
    def test_zip(self, minimnist_dataset: dict, key_format: str):
        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        )

        source_nkeys = len(minimnist_dataset["item_keys"])
        keys1 = minimnist_dataset["item_keys"][: source_nkeys // 2]
        keys2 = minimnist_dataset["item_keys"][source_nkeys // 2 :]  # noqa: E203
        seq1 = pls.SamplesSequence.from_list(  # type: ignore
            [sample.extract_keys(*keys1) for sample in source]
        )
        seq2 = pls.SamplesSequence.from_list(  # type: ignore
            [sample.extract_keys(*keys2) for sample in source]
        )

        eff_format = key_format if "*" in key_format else f"*{key_format}"
        fixstr = eff_format.replace("*", "")
        zipped = seq1.zip(seq2, key_format=key_format)
        assert len(zipped) == len(seq1)
        assert len(zipped) == len(seq2)
        for sample_m, sample_1, sample_2, sample_s in zip(zipped, seq1, seq2, source):
            assert len(sample_1.keys() & sample_2.keys()) == 0
            assert len(sample_1.keys() | sample_2.keys()) == len(sample_s.keys())
            assert len(sample_s.keys()) == len(sample_m.keys())
            assert all(k in sample_m for k in sample_1)
            assert all(eff_format.replace("*", k) in sample_m for k in sample_2)
            assert all(
                (eff_format.replace("*", k) if k in sample_2 else k) in sample_m
                for k in sample_s
            )
            assert all(k.replace(fixstr, "") in sample_s for k in sample_m)
            assert all(v is sample_m[k] for k, v in sample_1.items())
            assert all(
                v is sample_m[eff_format.replace("*", k)] for k, v in sample_2.items()
            )
            assert all(
                v is sample_m[(eff_format.replace("*", k) if k in sample_2 else k)]
                for k, v in sample_s.items()
            )
            assert all(
                v is sample_s[k.replace(fixstr, "")] for k, v in sample_m.items()
            )

    def _cat_test(self, minimnist_dataset: dict, fn):
        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        double_source = fn(source)

        length = len(source)
        assert 2 * length == len(double_source)
        for src_idx in range(length):
            assert source[src_idx] is double_source[src_idx]
            assert source[src_idx] is double_source[src_idx + length]

    def test_cat(self, minimnist_dataset: dict):
        self._cat_test(minimnist_dataset, lambda x: x.cat(x))

    def test_add(self, minimnist_dataset: dict):
        self._cat_test(minimnist_dataset, lambda x: x + x)

    def test_filter(self, minimnist_dataset: dict):
        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        filtered = source.filter(lambda x: x["label"] == 0)

        raw_count = len([sample for sample in source if sample["label"] == 0])
        assert raw_count == len(filtered)

        for sample in filtered:
            assert sample["label"] == 0

    def test_sort(self, minimnist_dataset: dict):
        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        ).sort(
            lambda x: -1 * x["label"]()  # type: ignore
        )

        last_label = 9
        for sample in source:
            assert sample["label"]() <= last_label  # type: ignore
            last_label = sample["label"]()

    def test_slice(self, minimnist_dataset: dict):
        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        start, stop, step = 4, 13, 3
        sliced = source.slice(start=start, stop=stop, step=step)
        raw_sliced = [source[idx] for idx in range(start, stop, step)]

        assert len(sliced) == len(raw_sliced)
        for s1, s2 in zip(sliced, raw_sliced):
            assert s1 is s2

    def test_slice_get(self, minimnist_dataset: dict):
        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        start, stop, step = 7, 18, 2
        sliced = source[start:stop:step]
        raw_sliced = [source[idx] for idx in range(start, stop, step)]

        assert len(sliced) == len(raw_sliced)
        for s1, s2 in zip(sliced, raw_sliced):
            assert s1 is s2

    def test_shuffle(self, minimnist_dataset: dict):
        seed = 42
        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        shuffled_1 = source.shuffle(seed=seed)
        shuffled_2 = source.shuffle(seed=seed)
        shuffled_3 = source.shuffle(seed=seed + 1)

        length = len(source)
        assert length == len(shuffled_1)
        assert length == len(shuffled_2)
        assert length == len(shuffled_3)
        assert any(source[idx] is not shuffled_1[idx] for idx in range(length))
        assert any(source[idx] is not shuffled_2[idx] for idx in range(length))
        assert any(source[idx] is not shuffled_3[idx] for idx in range(length))
        assert all(shuffled_1[idx] is shuffled_2[idx] for idx in range(length))
        assert any(shuffled_1[idx] is not shuffled_3[idx] for idx in range(length))

    def test_enumerate(self, minimnist_dataset: dict):
        import pipelime.items as pli

        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        enum_seq = source.enumerate(
            idx_key="custom_id", item_cls="pipelime.items.TxtNumpyItem"
        )

        for (idx, s_sample), e_sample in zip(enumerate(source), enum_seq):
            assert "custom_id" in e_sample
            assert isinstance(e_sample["custom_id"], pli.TxtNumpyItem)
            assert int(e_sample["custom_id"]()) == idx  # type: ignore
            assert all(v == e_sample[k] for k, v in s_sample.items())

    def test_repeat(self, minimnist_dataset: dict):
        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        repeat_seq = source.repeat(3)

        assert len(repeat_seq) == 3 * len(source)

        riter = iter(repeat_seq)
        for i in range(3):
            for s in source:
                assert next(riter) is s

        with pytest.raises(IndexError):
            repeat_seq[len(repeat_seq)]

    @pytest.mark.parametrize("indexes", [[1, 2, 5, 9], [-19, 2, -15, 9], [21, 22]])
    @pytest.mark.parametrize("negate", [True, False])
    def test_select(self, minimnist_dataset: dict, indexes: list, negate: bool):
        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        if any(idx >= len(source) for idx in indexes):
            with pytest.raises(ValueError):
                source.select(indexes, negate=negate)
        else:
            select_seq = source.select(indexes, negate=negate)
            effective_indexes = (
                [i for i in range(len(source)) if i not in indexes]
                if negate
                else indexes
            )

            assert len(select_seq) == len(effective_indexes)

            for i, s in zip(effective_indexes, select_seq):
                assert s is source[i]

    def _seq_cache(
        self,
        source_folder: Path,
        cache_folder: t.Optional[Path],
        reuse_cache: bool = False,
    ):
        import numpy as np
        from pipelime.stages import SampleStage

        class StageCounter(SampleStage):
            counter: int = 0

            def __call__(self, x):
                print("counter", self.counter)
                self.counter += 1
                x = x.duplicate_key("image", "img2")
                _ = x["img2"]()
                return x

        stage_counter = StageCounter()
        source = (
            pls.SamplesSequence.from_underfolder(
                folder=source_folder, merge_root_items=False
            )
            .map(stage_counter)
            .cache(cache_folder=cache_folder, reuse_cache=reuse_cache)
        )

        saved_samples = [x for x in source]
        for idx, x in enumerate(source):
            s = saved_samples[idx]
            assert x is not s
            assert x["image"]().size != 0  # type: ignore
            assert x["img2"]().size != 0  # type: ignore
            assert np.array_equal(x["image"](), s["image"](), equal_nan=True)  # type: ignore
            assert np.array_equal(x["img2"](), s["img2"](), equal_nan=True)  # type: ignore

        assert stage_counter.counter == (0 if reuse_cache else len(source))

    def test_cache(self, minimnist_dataset: dict):
        self._seq_cache(minimnist_dataset["path"], cache_folder=None)

    def test_cache_with_folder(self, minimnist_dataset: dict, tmp_path: Path):
        cache_folder = tmp_path / "cache_folder"
        self._seq_cache(minimnist_dataset["path"], cache_folder=cache_folder)
        self._seq_cache(
            minimnist_dataset["path"], cache_folder=cache_folder, reuse_cache=True
        )

        with pytest.raises(FileExistsError):
            self._seq_cache(
                minimnist_dataset["path"], cache_folder=cache_folder, reuse_cache=False
            )

    def test_item_data_cache(self, minimnist_dataset: dict):
        from pipelime.stages import StageLambda

        def _load_data(x, k):
            _ = x[k]()
            return x

        source = (
            pls.SamplesSequence.from_underfolder(folder=minimnist_dataset["path"])
            .map(StageLambda(lambda x: _load_data(x, "label")))
            .data_cache("NumpyItem")
            .map(StageLambda(lambda x: _load_data(x, "metadata")))
            .no_data_cache("Item")
        )

        for sample in source:
            assert "label" in sample
            assert "metadata" in sample
            assert sample["label"]._data_cache is not None
            assert sample["metadata"]._data_cache is None
