import typing as t
from pathlib import Path

import pytest

import pipelime.sequences as pls


class TestSamplesSequenceOperations:
    def test_base(self):
        from pipelime.sequences import Sample, SamplesSequence
        from pipelime.sequences.pipes import PipedSequenceBase

        src = SamplesSequence.from_list([Sample({}) for _ in range(10)])
        seq = PipedSequenceBase(source=src)
        assert len(seq) == len(src)
        assert all(s1 is s2 for s1, s2 in zip(seq, src))

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

    def _cat_test(self, minimnist_dataset: dict, fn: t.Callable, interleave: bool):
        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        eq_range = 5
        other_range = 4

        wr = eq_range
        xr = eq_range + other_range
        yr = len(source) - eq_range

        w = pls.SamplesSequence.from_list([s for s in source[:wr]])
        x = pls.SamplesSequence.from_list([s for s in source[wr:xr]])
        y = pls.SamplesSequence.from_list([s for s in source[xr:yr]])
        z = pls.SamplesSequence.from_list([s for s in source[yr:]])

        allcat = fn(w, x, y, z)

        assert len(source) == len(allcat)
        if interleave:
            from itertools import zip_longest

            samples = [
                s for smpls in zip_longest(w, x, y, z) for s in smpls if s is not None
            ]
            gtseq = pls.SamplesSequence.from_list(samples)
            assert len(gtseq) == len(allcat)
        else:
            gtseq = source

        for s1, s2 in zip(gtseq, allcat):
            assert s1 is s2

    @pytest.mark.parametrize(
        "fn",
        [
            lambda i, w, x, y, z: w.cat(x, y, z, interleave=i),
            lambda i, w, x, y, z: w.cat(to_cat=[x, y, z], interleave=i),
            lambda i, w, x, y, z: w.cat(x, to_cat=[y, z], interleave=i),
            lambda i, w, x, y, z: w.cat(x, y, to_cat=z, interleave=i),
        ],
    )
    @pytest.mark.parametrize("interleave", [False, True])
    def test_cat(self, minimnist_dataset: dict, fn: t.Callable, interleave: bool):
        from functools import partial

        self._cat_test(minimnist_dataset, partial(fn, interleave), interleave)

    @pytest.mark.parametrize(
        "fn",
        [
            lambda a, x, y, z: a + x + y + z,
            lambda a, x, y, z: (a + x) + (y + z),
            lambda a, x, y, z: a + (x + y + z),
            lambda a, x, y, z: a + (x + y) + z,
        ],
    )
    def test_add(self, minimnist_dataset: dict, fn):
        self._cat_test(minimnist_dataset, fn, False)

    @pytest.mark.parametrize("lazy", [True, False])
    @pytest.mark.parametrize("empty_smpls", [True, False])
    def test_filter(self, minimnist_dataset: dict, lazy, empty_smpls):
        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        filtered = source.filter(
            lambda x: x["label"]() == 0, lazy=lazy, insert_empty_samples=empty_smpls
        )

        if not empty_smpls:
            raw_count = len([sample for sample in source if sample["label"]() == 0])
            assert raw_count == len(filtered)
            for sample in filtered:
                assert sample["label"]() == 0
        else:
            assert len(source) == len(filtered)
            for fs, ss in zip(filtered, source):
                if ss["label"]() == 0:
                    assert fs["label"]() == 0
                else:
                    assert len(fs) == 0

    @pytest.mark.parametrize("lazy", [True, False])
    def test_sort(self, minimnist_dataset: dict, lazy):
        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        ).sort(lambda x: x.deep_get("metadata.random"), lazy=lazy)

        last_random = 0.0
        for sample in source:
            assert sample.deep_get("metadata.random") > last_random
            last_random = sample.deep_get("metadata.random")

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
            assert int(e_sample["custom_id"]()[0]) == idx  # type: ignore
            assert all(v == e_sample[k] for k, v in s_sample.items())

    @pytest.mark.parametrize("n", [1, 4, 10, 3.8, 4.2, 3.89])
    @pytest.mark.parametrize("interleave", [True, False])
    def test_repeat(self, minimnist_dataset: dict, n: int, interleave: bool):
        import math

        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        )
        repeat_seq = source.repeat(n, interleave=interleave)

        assert len(repeat_seq) == round(n * len(source))

        riter = iter(repeat_seq)
        if interleave:
            frac_size = len(repeat_seq) - len(source) * int(n)
            for sidx, s in enumerate(source):
                for i in range(math.ceil(n)) if sidx < frac_size else range(int(n)):
                    assert next(riter) is s
        else:
            for i in range(int(n)):
                for s in source:
                    assert next(riter) is s
            for s in source[: len(repeat_seq) - int(n) * len(source)]:
                assert next(riter) is s

        with pytest.raises(StopIteration):
            next(riter)

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

    @pytest.mark.parametrize("bsize,should_fail", [(3, False), (5, False), (0, True)])
    @pytest.mark.parametrize("drop_last", [True, False])
    @pytest.mark.parametrize("key_list", [None, ["image", "metadata"]])
    def test_batched(
        self, bsize, drop_last, should_fail, key_list, minimnist_dataset: dict
    ):
        from pydantic import ValidationError

        from pipelime.items import NumpyItem

        from ... import TestUtils

        source = pls.SamplesSequence.from_underfolder(folder=minimnist_dataset["path"])

        try:
            batched = source.batched(
                batch_size=bsize, drop_last=drop_last, key_list=key_list
            )
            assert not should_fail
        except ValidationError:
            assert should_fail
            return

        assert len(batched) == len(source) // bsize + min(
            int(not drop_last), len(source) % bsize
        )

        sidx = 0
        for batch in batched:
            sb = source[sidx : sidx + bsize]
            for k in sb[0].keys():
                if not key_list or k in key_list:
                    assert k in batch

            for k, v in batch.items():
                assert k in sb[0]
                if v.is_shared:
                    assert v is sb[0][k]
                else:
                    x = v()
                    assert len(x) == len(sb)
                    for i, y in enumerate(x):
                        if isinstance(v, NumpyItem):
                            TestUtils.numpy_eq(y, sb[i][k]())
                        else:
                            assert y == sb[i][k]()

            sidx += bsize

    @pytest.mark.parametrize("key_list", [None, ["image", "metadata"]])
    @pytest.mark.parametrize(
        "ubsize,drop_last,should_fail",
        [
            (4, True, False),
            ("fixed", True, False),
            ("variable", True, False),
            (4, False, True),
            ("fixed", False, True),
            ("variable", False, False),
        ],
    )
    def test_unbatched(self, key_list, ubsize, drop_last, should_fail):
        from pipelime.items import NumpyItem

        from ... import TestUtils

        SOURCE_LENGTH = 15
        SOURCE_BSIZE = 4

        toy = pls.SamplesSequence.toy_dataset(
            length=SOURCE_LENGTH,
            with_images=True,
            with_masks=False,
            with_instances=False,
            with_objects=False,
            with_bboxes=False,
            with_kpts=False,
        )
        btoy = toy.batched(batch_size=SOURCE_BSIZE, drop_last=drop_last)
        ub = btoy.unbatched(batch_size=ubsize, key_list=key_list)

        if ubsize != "variable":
            assert len(ub) == len(btoy) * SOURCE_BSIZE
        elif drop_last:
            assert len(ub) == SOURCE_BSIZE * (len(toy) // SOURCE_BSIZE)
        else:
            assert len(ub) == len(toy)

        try:
            _ = ub[-1]
            assert not should_fail
        except Exception:
            assert should_fail
            ub = ub[:-1]

        for x, y in zip(ub, toy):
            if key_list:
                assert set(x.keys()) == set(key_list)
                y = y.extract_keys(*key_list)

            assert x.keys() == y.keys()

            for k, v1 in x.items():
                v2 = y[k]
                if isinstance(v1, NumpyItem):
                    assert TestUtils.numpy_eq(v1(), v2())
                else:
                    assert v1() == v2()
