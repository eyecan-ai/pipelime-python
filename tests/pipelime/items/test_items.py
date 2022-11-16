import pytest
from pathlib import Path
import numpy as np
import typing as t

import trimesh
import trimesh.creation

import pipelime.items as pli


def _np_eq(x, y) -> bool:
    return np.array_equal(x, y, equal_nan=True)


def _np_eq_1d(x, y) -> bool:
    return np.array_equal(np.atleast_1d(x), y, equal_nan=True)


def _mesh_eq(x, y, exact: bool, sort_vertices: bool) -> bool:
    if isinstance(x, trimesh.Scene):
        if len(x.geometry) != 1:
            return False
        x = next(iter(x.geometry.values()))
    if isinstance(y, trimesh.Scene):
        if len(y.geometry) != 1:
            return False
        y = next(iter(y.geometry.values()))

    faces_eq = True if sort_vertices else _np_eq(x.faces, y.faces)

    vx, vy = (
        (np.sort(x.vertices, axis=None), np.sort(y.vertices, axis=None))
        if sort_vertices
        else (x.vertices, y.vertices)
    )
    return faces_eq and (
        _np_eq(vx, vy) if exact else np.allclose(vx, vy, equal_nan=True)
    )


class TestItems:
    def test_reference_items(self, items_folder: Path):
        from pipelime.items.base import ItemFactory

        checked_items = set()
        for fp in items_folder.iterdir():
            if fp not in checked_items:
                print("reference item:", fp)
                checked_items.add(fp)
                ref_item = ItemFactory.get_instance(fp)
                ref_value = ref_item()

                trg_files = list(items_folder.glob(fp.stem + ".*")) + list(
                    items_folder.glob(fp.stem + "[a-z].*")
                )
                for other in trg_files:
                    if other not in checked_items:
                        print("target item:", other)
                        checked_items.add(other)
                        trg_item = ItemFactory.get_instance(other)
                        trg_value = trg_item()

                        if isinstance(ref_value, np.ndarray):
                            assert _np_eq(ref_value, trg_value)
                        elif isinstance(ref_value, tuple):  # the pickle file is a tuple
                            assert ref_value[3] == trg_value
                        elif isinstance(trg_value, tuple):
                            assert ref_value == trg_value[3]
                        elif isinstance(ref_item, pli.STLModel3DItem) or isinstance(
                            trg_item, pli.STLModel3DItem
                        ):
                            assert _mesh_eq(
                                ref_value, trg_value, exact=True, sort_vertices=True
                            )
                        elif isinstance(ref_item, pli.OFFModel3DItem) or isinstance(
                            trg_item, pli.OFFModel3DItem
                        ):
                            assert _mesh_eq(
                                ref_value, trg_value, exact=False, sort_vertices=False
                            )
                        elif isinstance(ref_item, pli.Model3DItem):
                            assert _mesh_eq(
                                ref_value, trg_value, exact=True, sort_vertices=False
                            )
                        else:
                            assert ref_value == trg_value

    @pytest.mark.parametrize(
        ["item_cls", "value", "eq_fn"],
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
            (
                pli.STLModel3DItem,
                trimesh.creation.box(),
                lambda x, y: _mesh_eq(x, y, exact=True, sort_vertices=True),
            ),
            (
                pli.OBJModel3DItem,
                trimesh.creation.box(),
                lambda x, y: _mesh_eq(x, y, exact=True, sort_vertices=False),
            ),
            (
                pli.PLYModel3DItem,
                trimesh.creation.box(),
                lambda x, y: _mesh_eq(x, y, exact=True, sort_vertices=False),
            ),
            (
                pli.OFFModel3DItem,
                trimesh.creation.box(),
                lambda x, y: _mesh_eq(x, y, exact=False, sort_vertices=False),
            ),
            (
                pli.GLBModel3DItem,
                trimesh.creation.box(),
                lambda x, y: _mesh_eq(x, y, exact=True, sort_vertices=False),
            ),
        ],
    )
    def test_read_write(self, tmp_path: Path, item_cls: t.Type[pli.Item], value, eq_fn):
        value_path = tmp_path / "value"

        w_item = item_cls(value)
        w_item.serialize(value_path)
        assert eq_fn(value, w_item())

        value_path = value_path.with_suffix(item_cls.file_extensions()[0])
        r_item = item_cls(value_path)
        assert eq_fn(w_item(), r_item())

        with value_path.open("rb") as f:
            br_item = item_cls(f)
            assert eq_fn(w_item(), br_item())

    def test_read_write_jpeg(self, tmp_path: Path):
        import imageio.v3 as iio
        from io import BytesIO

        data_path = tmp_path / "data"
        data = (np.random.rand(3, 4) * 100).astype(np.uint8)

        w_item = pli.JpegImageItem(data)
        w_item.serialize(data_path)
        assert _np_eq(data, w_item())

        jpeg_suffix = pli.JpegImageItem.file_extensions()[0]
        r_item = pli.JpegImageItem(data_path.with_suffix(jpeg_suffix))

        encoded = BytesIO()
        iio.imwrite(
            encoded, data, extension=jpeg_suffix, **pli.JpegImageItem.save_options()
        )
        encoded.seek(0)
        decoded = np.array(iio.imread(encoded, extension=jpeg_suffix))

        assert _np_eq(r_item(), decoded)

    def test_data_cache(self, items_folder: Path):
        from pipelime.items.base import ItemFactory

        for v in ItemFactory.ITEM_DATA_CACHE_MODE.values():
            assert v is None

        def _reload() -> t.Tuple[pli.Item, pli.Item]:
            bmp_item = ItemFactory.get_instance(items_folder / "0.bmp")
            json_item = ItemFactory.get_instance(items_folder / "3.json")
            return bmp_item, json_item

        def _check_single(item, should_cache, is_cached):
            item.cache_data = should_cache
            assert item.cache_data is should_cache
            _ = item()
            if is_cached:
                assert item._data_cache is not None
            else:
                assert item._data_cache is None

        def _check(should_cache, is_cached):
            for id, it in enumerate(_reload()):
                _check_single(it, should_cache[id], is_cached[id])

        _check((True, True), (True, True))
        _check((False, False), (False, False))

        with pli.no_data_cache(pli.NumpyItem):
            _check((True, True), (True, True))
            _check((None, None), (False, True))

        with pli.no_data_cache(pli.NumpyItem, pli.JsonMetadataItem):
            pli.enable_item_data_cache(pli.NumpyItem)
            _check((True, True), (True, True))
            _check((False, True), (False, True))
            _check((None, None), (True, False))
            _check((None, None), (True, False))

    def test_set_data_twice(self):
        with pytest.raises(ValueError):
            _ = pli.NpyNumpyItem(np.random.rand(3, 4), np.random.rand(3, 4))

    def test_set_from_binary(self, items_folder: Path):
        with (items_folder / "2.txt").open("rb") as fp:
            ref = pli.TxtNumpyItem(fp)()
            gt = np.array(
                [
                    [
                        6.390370900299208179e-01,
                        4.262250008515110489e-01,
                        9.315973628491435177e-01,
                        1.002942906369641562e-01,
                    ],
                    [
                        7.517292051086260640e-01,
                        1.087911338619208523e-01,
                        9.393092045579851668e-01,
                        2.912748772468053415e-01,
                    ],
                    [
                        3.411277654275945981e-01,
                        8.135318888374636348e-01,
                        7.341307526247440318e-01,
                        1.800992012876649895e-01,
                    ],
                ]
            )

            assert isinstance(ref, np.ndarray)
            assert gt.shape == ref.shape
            assert np.all(gt - ref < 1e-6)  # type: ignore

    def test_invalid_ext(self):
        with pytest.raises(ValueError):
            _ = pli.NpyNumpyItem(Path("foo.bar"))

    def test_invalid_source(self):
        from urllib.parse import ParseResult

        item = pli.NpyNumpyItem(
            Path("foo.npy"),
            ParseResult(
                scheme="foo",
                netloc="bar",
                path="baz.npy",
                params="",
                query="",
                fragment="",
            ),
        )
        assert item() is None

    def test_disabled_serialization_modes(  # noqa: C901
        self, minimnist_private_dataset: dict, tmp_path: Path
    ):
        import pipelime.items as pli
        from pipelime.sequences import SamplesSequence
        from pipelime.stages import StageUploadToRemote
        from pipelime.remotes import make_remote_url

        # data lake
        remote_url = make_remote_url(
            scheme="file",
            host="localhost",
            bucket=(tmp_path / "rmbucket"),
        )

        input_seq = SamplesSequence.from_underfolder(  # type: ignore
            minimnist_private_dataset["path"], merge_root_items=True
        )

        # upload to remote (remote source is added to the items)
        for _ in input_seq.map(
            StageUploadToRemote(remotes=[remote_url])  # type: ignore
        ):
            pass

        # default mode: writing remote files
        for _ in input_seq.to_underfolder(folder=(tmp_path / "output_rm")):
            pass
        for p in (tmp_path / "output_rm").rglob("*"):
            if p.is_file():
                assert p.suffix == pli.Item.REMOTE_FILE_EXT

        # disable writing remote files
        with pli.item_disabled_serialization_modes("REMOTE_FILE"):
            for _ in input_seq.to_underfolder(folder=(tmp_path / "output_hl")):
                pass
            for p in (tmp_path / "output_hl").rglob("*"):
                if p.is_file():
                    assert p.suffix != pli.Item.REMOTE_FILE_EXT
                    assert not p.is_symlink()
                    assert p.stat().st_nlink > 1

        with pli.item_disabled_serialization_modes(
            pli.SerializationMode.REMOTE_FILE, pli.NumpyItem
        ):
            with pli.item_disabled_serialization_modes(
                [pli.SerializationMode.DEEP_COPY, "HARD_LINK"], pli.ImageItem
            ):
                for _ in input_seq.to_underfolder(folder=(tmp_path / "output_mx")):
                    pass
                for p in (tmp_path / "output_mx").rglob("*"):
                    if p.is_file():
                        assert not p.is_symlink()
                        if p.suffix == pli.TxtNumpyItem.file_extensions()[0]:
                            assert p.stat().st_nlink > 1
                        elif p.suffix == pli.PngImageItem.file_extensions()[0]:
                            assert p.stat().st_nlink == 1
                        else:
                            format_suffix = (
                                pli.JsonMetadataItem.file_extensions()[0]
                                if ".json" in p.suffixes
                                else pli.YamlMetadataItem.file_extensions()[0]
                            )
                            assert (
                                "".join(p.suffixes)
                                == format_suffix + pli.Item.REMOTE_FILE_EXT
                            )
