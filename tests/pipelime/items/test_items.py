import pytest
from pathlib import Path
import numpy as np
import typing as t

import pipelime.items as pli


def _np_eq(x, y) -> bool:
    return np.array_equal(x, y, equal_nan=True)


def _np_eq_1d(x, y) -> bool:
    return np.array_equal(np.atleast_1d(x), y, equal_nan=True)


class TestItems:
    def test_reference_items(self, items_folder: Path):
        from pipelime.items.base import ItemFactory

        checked_items = set()
        for fp in items_folder.iterdir():
            if fp not in checked_items:
                checked_items.add(fp)
                ref_item = ItemFactory.get_instance(fp)()
                for other in items_folder.glob(fp.stem + ".*"):
                    if other not in checked_items:
                        checked_items.add(other)
                        trg_item = ItemFactory.get_instance(fp)()

                        if isinstance(ref_item, np.ndarray):
                            assert _np_eq(ref_item, trg_item)
                        elif isinstance(ref_item, tuple):  # the pickle file is a tuple
                            assert ref_item[3] == trg_item
                        elif isinstance(trg_item, tuple):
                            assert ref_item == trg_item[3]
                        else:
                            assert ref_item == trg_item

    @pytest.mark.parametrize(
        "item_cls,value,eq_fn",
        [
            (pli.PickleItem, (42, "asdf", 3.14), lambda x, y: x == y),
            (pli.BinaryItem, b"asdfiasodifoj123124214", lambda x, y: x == y),
            (pli.NpyNumpyItem, (np.random.rand(3, 4) * 100), _np_eq),
            (pli.TxtNumpyItem, (np.random.rand(3, 4) * 100), _np_eq_1d),
            (pli.NpyNumpyItem, 42.42, _np_eq),
            (pli.TxtNumpyItem, 42.42, _np_eq_1d),
            (pli.BmpImageItem, (np.random.rand(3, 4) * 100).astype(np.uint8), _np_eq),
            (pli.PngImageItem, (np.random.rand(3, 4) * 100).astype(np.uint8), _np_eq),
            (pli.TiffImageItem, (np.random.rand(3, 4) * 100).astype(np.uint8), _np_eq),
            (
                pli.JsonMetadataItem,
                {"a": [1, 2, 3], "b": 3.14, "c": [True, False]},
                lambda x, y: x == y,
            ),
            (
                pli.YamlMetadataItem,
                {"a": [1, 2, 3], "b": 3.14, "c": [True, False]},
                lambda x, y: x == y,
            ),
            (
                pli.TomlMetadataItem,
                {"a": [1, 2, 3], "b": 3.14, "c": [True, False]},
                lambda x, y: x == y,
            ),
        ],
    )
    def test_read_write(self, tmp_path: Path, item_cls: t.Type[pli.Item], value, eq_fn):
        value_path = tmp_path / "value"

        w_item = item_cls(value)
        w_item.serialize(value_path)
        assert eq_fn(value, w_item())

        r_item = item_cls(value_path.with_suffix(item_cls.file_extensions()[0]))
        assert eq_fn(w_item(), r_item())

    def test_read_write_jpeg(self, tmp_path: Path):
        import imageio
        from io import BytesIO

        data_path = tmp_path / "data"
        data = (np.random.rand(3, 4) * 100).astype(np.uint8)

        w_item = pli.JpegImageItem(data)
        w_item.serialize(data_path)
        assert _np_eq(data, w_item())

        jpeg_suffix = pli.JpegImageItem.file_extensions()[0]
        r_item = pli.JpegImageItem(data_path.with_suffix(jpeg_suffix))

        encoded = BytesIO()
        imageio.imwrite(
            encoded, data, format=jpeg_suffix, **pli.JpegImageItem.save_options()
        )
        encoded.seek(0)
        decoded = np.array(imageio.imread(encoded, format=jpeg_suffix))

        assert _np_eq(r_item(), decoded)
