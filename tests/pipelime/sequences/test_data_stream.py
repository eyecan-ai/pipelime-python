import pytest
from pathlib import Path
from pipelime.sequences import SamplesSequence, DataStream


class TestDataStream:
    def _fill_from_source(self, data_stream, output_path, source_path, keys):
        source = SamplesSequence.from_underfolder(source_path)
        for idx, sample in enumerate(source):
            data_stream.set_output(idx, sample, keys=keys)

        self._check_output_folder(output_path, data_stream, source, set(), set(keys))
        return source

    def _check_stream_reading(self, data_stream, data_len):
        import pipelime.items as pli
        import numpy as np

        original_samples = [data_stream.get_input(i) for i in range(data_len)]
        for s1, s2 in zip(original_samples, data_stream):
            assert s1.keys() == s2.keys()
            for k, v1 in s1.items():
                v2 = s2[k]
                assert v1.__class__ == v2.__class__
                if isinstance(v1, pli.NumpyItem):
                    assert np.array_equal(v1(), v2(), equal_nan=True)  # type: ignore
                else:
                    assert v1() == v2()
        return original_samples

    def _check_num_samples(self, dataset_info: dict, extra_sample_items: int):
        import os

        # NB: `scandir` includes the `data` folder
        root_files = sum(1 for _ in os.scandir(dataset_info["path"].as_posix()))
        assert root_files == len(dataset_info["root_keys"]) + 1

        item_files = sum(
            1 for _ in os.scandir((dataset_info["path"] / "data").as_posix())
        )
        assert (
            item_files // (len(dataset_info["item_keys"]) + extra_sample_items)
        ) == dataset_info["len"]

    def _check_output_folder(
        self,
        output_path,
        data_stream,
        original_samples,
        extra_keys,
        reduced_keys=None,
        custom_check_fn=None,
    ):
        import pipelime.items as pli
        import numpy as np

        new_reader = SamplesSequence.from_underfolder(output_path)
        for (idx, olds), strms, news in zip(
            enumerate(original_samples), data_stream, new_reader
        ):
            assert strms.keys() == news.keys()
            assert (
                olds.keys() & reduced_keys if reduced_keys else olds.keys()
            ) | extra_keys == strms.keys()
            for k, v1 in olds.items():
                if not reduced_keys or k in reduced_keys:
                    v2 = news[k]
                    assert v1.__class__ == v2.__class__
                    if custom_check_fn is None or not custom_check_fn(idx, k, v1, v2):
                        if isinstance(v1, pli.NumpyItem):
                            assert np.array_equal(v1(), v2(), equal_nan=True)  # type: ignore
            for e in extra_keys:
                assert strms[e]() == news[e]()

    @pytest.mark.parametrize("zfill", [None, 6])
    def test_read_write_stream(self, minimnist_private_dataset: dict, zfill):
        import pipelime.items as pli
        import numpy as np

        # create a r/w stream
        data_stream = DataStream.read_write_underfolder(
            minimnist_private_dataset["path"], zfill=zfill
        )
        data_len = minimnist_private_dataset["len"]
        assert len(data_stream) == data_len

        # check multiple reading passes
        original_samples = self._check_stream_reading(data_stream, data_len)

        # add a new item to all samples (and write it out)
        for i in range(data_len):
            sample = original_samples[i].set_item(
                "extra", pli.JsonMetadataItem({"extra_value": 42 + i})
            )
            data_stream.set_output(i, sample)

        # check that the new item is present in all samples
        self._check_output_folder(
            minimnist_private_dataset["path"], data_stream, original_samples, {"extra"}
        )

        # check that files with different zfill have not been duplicated
        self._check_num_samples(minimnist_private_dataset, extra_sample_items=1)

        # change an existing item and write a subset of the keys
        img_key = minimnist_private_dataset["image_keys"][0]
        org_imgs = [s[img_key]() for s in original_samples]
        for i in range(data_len):
            new_image: np.ndarray = 255 - org_imgs[i]  # type: ignore
            sample = original_samples[i].set_value(img_key, new_image)
            data_stream.set_output(
                i, sample, keys=minimnist_private_dataset["image_keys"]
            )

        # check againg the output folder (the image has been inverted)
        def _custom_check_fn(idx, k, v1, v2):
            if k == img_key:
                assert np.array_equal(255 - org_imgs[idx], v2(), equal_nan=True)  # type: ignore
                return True
            return False

        self._check_output_folder(
            minimnist_private_dataset["path"],
            data_stream,
            original_samples,
            {"extra"},
            None,
            _custom_check_fn,
        )

        # check that files with different zfill have not been duplicated
        self._check_num_samples(minimnist_private_dataset, extra_sample_items=1)

    def test_create_new_stream(self, minimnist_dataset: dict, tmp_path: Path):
        output_path = str(tmp_path / "output")
        data_stream = DataStream.create_new_underfolder(output_path, zfill=2)
        self._fill_from_source(
            data_stream,
            output_path,
            minimnist_dataset["path"],
            [minimnist_dataset["image_keys"][0]],
        )

        with pytest.raises(FileExistsError):
            _ = DataStream.create_new_underfolder(output_path)

    def test_output_stream(self, minimnist_dataset: dict, tmp_path: Path):
        output_path = str(tmp_path / "output")
        data_stream = DataStream.create_output_stream(output_path, zfill=2)
        self._fill_from_source(
            data_stream,
            output_path,
            minimnist_dataset["path"],
            [minimnist_dataset["image_keys"][0]],
        )

        assert len(data_stream) == 0
        with pytest.raises(IndexError):
            _ = data_stream.get_input(0)
