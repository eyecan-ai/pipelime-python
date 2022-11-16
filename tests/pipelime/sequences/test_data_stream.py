import pytest
from pipelime.sequences import SamplesSequence, DataStream


class TestDataStream:
    @pytest.mark.parametrize("zfill", [None, 6])
    def test_read_write_stream(self, minimnist_private_dataset: dict, zfill):
        import pipelime.items as pli
        import numpy as np
        import os

        data_stream = DataStream.read_write_underfolder(
            minimnist_private_dataset["path"], zfill=zfill
        )
        data_len = minimnist_private_dataset["len"]
        assert len(data_stream) == data_len

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

        for i in range(data_len):
            sample = original_samples[i].set_item(
                "extra", pli.JsonMetadataItem({"extra_value": 42 + i})
            )
            data_stream.set_output(i, sample)

        new_reader = SamplesSequence.from_underfolder(minimnist_private_dataset["path"])
        for olds, strms, news in zip(original_samples, data_stream, new_reader):
            assert strms.keys() == news.keys()
            assert olds.keys() | ["extra"] == strms.keys()
            for k, v1 in olds.items():
                v2 = news[k]
                assert v1.__class__ == v2.__class__
                if isinstance(v1, pli.NumpyItem):
                    assert np.array_equal(v1(), v2(), equal_nan=True)  # type: ignore
            assert strms["extra"]() == news["extra"]()

        root_files = sum(
            1 for _ in os.scandir(minimnist_private_dataset["path"].as_posix())
        )
        assert root_files == len(minimnist_private_dataset["root_keys"]) + 1

        item_files = sum(
            1
            for _ in os.scandir((minimnist_private_dataset["path"] / "data").as_posix())
        )
        assert (
            item_files // (len(minimnist_private_dataset["item_keys"]) + 1)
        ) == minimnist_private_dataset["len"]
