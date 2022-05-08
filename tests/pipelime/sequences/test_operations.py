import pipelime.sequences as pls
import typing as t


class TestSamplesSequenceOperations:
    def test_map(self, minimnist_dataset: dict):
        import pipelime.items as pli
        import pipelime.stages as plst

        source = pls.SamplesSequence.from_underfolder(  # type: ignore
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

    def test_zip(self, minimnist_dataset: dict):
        source = pls.SamplesSequence.from_underfolder(  # type: ignore
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

        zipped = seq1.zip(seq2)
        assert len(zipped) == len(seq1)
        assert len(zipped) == len(seq2)
        for sample_m, sample_1, sample_2, sample_s in zip(zipped, seq1, seq2, source):
            assert len(sample_1.keys() & sample_2.keys()) == 0
            assert len(sample_1.keys() | sample_2.keys()) == len(sample_s.keys())
            assert len(sample_s.keys()) == len(sample_m.keys())
            assert all(k in sample_m for k in sample_1)
            assert all(k in sample_m for k in sample_2)
            assert all(k in sample_m for k in sample_s)
            assert all(k in sample_s for k in sample_m)
            assert all(v is sample_m[k] for k, v in sample_1.items())
            assert all(v is sample_m[k] for k, v in sample_2.items())
            assert all(v is sample_m[k] for k, v in sample_s.items())
            assert all(v is sample_s[k] for k, v in sample_m.items())

    def _cat_test(self, minimnist_dataset: dict, fn):
        source = pls.SamplesSequence.from_underfolder(  # type: ignore
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
        source = pls.SamplesSequence.from_underfolder(  # type: ignore
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        filtered = source.filter(lambda x: x["label"] == 0)

        raw_count = len([sample for sample in source if sample["label"] == 0])
        assert raw_count == len(filtered)

        for sample in filtered:
            assert sample["label"] == 0

    def test_sort(self, minimnist_dataset: dict):
        source = pls.SamplesSequence.from_underfolder(  # type: ignore
            folder=minimnist_dataset["path"], merge_root_items=False
        ).sort(lambda x: -1 * x["label"]())

        last_label = 9
        for sample in source:
            assert sample["label"]() <= last_label
            last_label = sample["label"]()

    def test_slice(self, minimnist_dataset: dict):
        source = pls.SamplesSequence.from_underfolder(  # type: ignore
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        start, stop, step = 4, 13, 3
        sliced = source.slice(start=start, stop=stop, step=step)
        raw_sliced = [source[idx] for idx in range(start, stop, step)]

        assert len(sliced) == len(raw_sliced)
        for s1, s2 in zip(sliced, raw_sliced):
            assert s1 is s2

    def test_slice_get(self, minimnist_dataset: dict):
        source = pls.SamplesSequence.from_underfolder(  # type: ignore
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
        source = pls.SamplesSequence.from_underfolder(  # type: ignore
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

        source = pls.SamplesSequence.from_underfolder(  # type: ignore
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        enum_seq = source.enumerate(
            idx_key="custom_id", item_cls_path="pipelime.items.TxtNumpyItem"
        )

        for (idx, s_sample), e_sample in zip(enumerate(source), enum_seq):
            assert "custom_id" in e_sample
            assert isinstance(e_sample["custom_id"], pli.TxtNumpyItem)
            assert int(e_sample["custom_id"]()) == idx
            assert all(v == e_sample[k] for k, v in s_sample.items())

    def test_repeat(self, minimnist_dataset: dict):
        source = pls.SamplesSequence.from_underfolder(  # type: ignore
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        repeat_seq = source.repeat(3)

        assert len(repeat_seq) == 3 * len(source)

        riter = iter(repeat_seq)
        for i in range(3):
            for s in source:
                assert next(riter) is s
