import typing as t
from pathlib import Path

import numpy as np
import pytest
import trimesh

import pipelime.items as pli

from ... import TestUtils


def _generic_mesh():
    import math

    import trimesh.creation

    c, s = math.cos(30 * math.pi / 180), math.sin(30 * math.pi / 180)
    rz = np.array([[c, -s, 0, 0], [s, c, 0, 0], [0, 0, 1, 0], [0, 0, 0, 1]])

    return trimesh.creation.cone(radius=1.0, height=2.0, sections=5, transform=rz)


def _generic_scene():
    mesh = _generic_mesh()
    return trimesh.Scene(mesh)


def _to_mesh(x):
    if isinstance(x, trimesh.Scene):
        if len(x.geometry) == 1:
            x = next(iter(x.geometry.values()))
    if isinstance(x, trimesh.Trimesh):
        return x
    return None


def _sorted_faces(sortidx, faces):
    def _sorted_faces_col(sortidx, faces, col):
        return np.array(
            [np.argwhere(sortidx == idx).squeeze() for idx in faces[:, col]]
        )

    return np.stack(
        [
            _sorted_faces_col(sortidx, faces, 0),
            _sorted_faces_col(sortidx, faces, 1),
            _sorted_faces_col(sortidx, faces, 2),
        ],
        axis=-1,
    )


def _mesh_eq(x, y, exact: bool, sort_vertices: bool) -> bool:
    x, y = _to_mesh(x), _to_mesh(y)
    if x is None or y is None:
        return False

    print("vs-before", x.vertices, y.vertices, sep="\n")

    if sort_vertices:
        x_sort, y_sort = x.vertices[:, 0].argsort(), y.vertices[:, 0].argsort()
        vx, vy = x.vertices[x_sort], y.vertices[y_sort]
        fx, fy = _sorted_faces(x_sort, x.faces), _sorted_faces(y_sort, y.faces)
    else:
        vx, vy = x.vertices, y.vertices
        fx, fy = x.faces, y.faces

    print("vs-after", vx, vy, sep="\n")
    print("vs", vx - vy, sep="\n")
    print("fs", fx - fy, sep="\n")

    return TestUtils.numpy_eq(fx, fy) and (
        TestUtils.numpy_eq(vx, vy) if exact else np.allclose(vx, vy, equal_nan=True)
    )


class TestItems:
    def test_reference_items(self, items_folder: Path):
        checked_items = set()
        for fp in items_folder.iterdir():
            if fp not in checked_items:
                print("reference item:", fp)
                checked_items.add(fp)
                ref_item = pli.Item.get_instance(fp)
                ref_value = ref_item()

                trg_files = list(items_folder.glob(fp.stem + ".*")) + list(
                    items_folder.glob(fp.stem + "[a-z].*")
                )
                for other in trg_files:
                    if other not in checked_items:
                        print("target item:", other)
                        checked_items.add(other)
                        trg_item = pli.Item.get_instance(other)
                        trg_value = trg_item()

                        if isinstance(ref_value, np.ndarray):
                            assert TestUtils.numpy_eq(ref_value, trg_value)
                        elif isinstance(ref_value, tuple):  # the pickle file is a tuple
                            assert ref_value[3] == trg_value
                        elif isinstance(trg_value, tuple):
                            assert ref_value == trg_value[3]
                        elif isinstance(ref_item, pli.Model3DItem):
                            exact = not isinstance(
                                ref_item, pli.OFFModel3DItem
                            ) and not isinstance(trg_item, pli.OFFModel3DItem)
                            sort_vtx = isinstance(
                                ref_item, pli.STLModel3DItem
                            ) or isinstance(trg_item, pli.STLModel3DItem)
                            assert _mesh_eq(
                                ref_value,
                                trg_value,
                                exact=exact,
                                sort_vertices=sort_vtx,
                            )
                        else:
                            assert ref_value == trg_value

    @pytest.mark.parametrize(
        ["item_cls", "value", "eq_fn"],
        [
            (pli.PickleItem, (42, "asdf", 3.14), lambda x, y: x == y),
            (pli.BinaryItem, b"asdfiasodifoj123124214", lambda x, y: x == y),
            (pli.NpyNumpyItem, (np.random.rand(3, 4) * 100), TestUtils.numpy_eq),
            (
                pli.TxtNumpyItem,
                (np.random.rand(3, 4) * 100),
                lambda x, y: TestUtils.numpy_eq(np.atleast_1d(x), y),
            ),
            (pli.NpyNumpyItem, 42.42, TestUtils.numpy_eq),
            (
                pli.TxtNumpyItem,
                42.42,
                lambda x, y: TestUtils.numpy_eq(np.atleast_1d(x), y),
            ),
            (
                pli.BmpImageItem,
                (np.random.rand(3, 4) * 100).astype(np.uint8),
                TestUtils.numpy_eq,
            ),
            (
                pli.PngImageItem,
                (np.random.rand(3, 4) * 100).astype(np.uint8),
                TestUtils.numpy_eq,
            ),
            (
                pli.TiffImageItem,
                (np.random.rand(3, 4) * 100).astype(np.uint8),
                TestUtils.numpy_eq,
            ),
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
                _generic_mesh(),
                lambda x, y: _mesh_eq(x, y, exact=False, sort_vertices=True),
            ),
            (
                pli.OBJModel3DItem,
                _generic_mesh(),
                lambda x, y: _mesh_eq(x, y, exact=False, sort_vertices=False),
            ),
            (
                pli.PLYModel3DItem,
                _generic_mesh(),
                lambda x, y: _mesh_eq(x, y, exact=False, sort_vertices=False),
            ),
            (
                pli.OFFModel3DItem,
                _generic_mesh(),
                lambda x, y: _mesh_eq(x, y, exact=False, sort_vertices=False),
            ),
            (
                pli.GLBModel3DItem,
                _generic_scene(),
                lambda x, y: _mesh_eq(x, y, exact=False, sort_vertices=False),
            ),
            (
                pli.GLTFModel3DItem,
                _generic_scene(),
                lambda x, y: _mesh_eq(x, y, exact=False, sort_vertices=False),
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
        assert r_item.local_sources == [value_path]

        with value_path.open("rb") as f:
            br_item = item_cls(f)
            assert eq_fn(w_item(), br_item())

    def test_read_write_jpeg(self, tmp_path: Path):
        from io import BytesIO

        import imageio.v3 as iio

        data_path = tmp_path / "data"
        data = (np.random.rand(3, 4) * 100).astype(np.uint8)

        with pli.data_cache(pli.JpegImageItem):
            w_item = pli.JpegImageItem(data)
            w_item.serialize(data_path)
            assert TestUtils.numpy_eq(data, w_item())

        jpeg_suffix = pli.JpegImageItem.file_extensions()[0]
        r_item = pli.JpegImageItem(data_path.with_suffix(jpeg_suffix))

        encoded = BytesIO()
        iio.imwrite(
            encoded, data, extension=jpeg_suffix, **pli.JpegImageItem.save_options()
        )
        encoded.seek(0)
        decoded = np.array(iio.imread(encoded, extension=jpeg_suffix))

        assert TestUtils.numpy_eq(r_item(), decoded)

    def test_read_write_unknown(self, tmp_path: Path):
        text = "random data"
        source_path = tmp_path / "source.unk"
        dest_req_path = tmp_path / "dest.rnd"
        dest_path = tmp_path / "dest.unk"
        other_path = tmp_path / "other"

        source_path.write_text(text)

        item = pli.UnknownItem(source_path)
        assert item() is None

        item.serialize(dest_req_path)

        assert not dest_req_path.exists()
        assert dest_path.exists()
        assert dest_path.read_text() == text
        assert not dest_path.is_symlink()
        assert dest_path.stat().st_nlink == 2
        assert source_path.stat().st_nlink == 2

        item = pli.Item.get_instance(dest_path)
        item.serialization_mode = pli.SerializationMode.DEEP_COPY
        item.serialize(other_path)

        other_path = other_path.with_suffix(".unk")

        assert other_path.exists()
        assert other_path.read_text() == text
        assert not other_path.is_symlink()
        assert other_path.stat().st_nlink == 1
        assert dest_path.stat().st_nlink == 2
        assert source_path.stat().st_nlink == 2

    def test_data_cache(self, items_folder: Path, tmp_path: Path):
        for v in pli.Item.ITEM_DATA_CACHE_MODE.values():
            assert v is None

        def _reload():
            bmp_item = pli.Item.get_instance(items_folder / "0.bmp")
            json_item = pli.Item.get_instance(items_folder / "3.json")
            npy_local_item = pli.NpyNumpyItem(np.array([1, 2, 3]))
            npy_local_item.serialize(tmp_path / "local.npy")
            txt_local_item = pli.TxtNumpyItem(np.array([1, 2, 3]))
            return bmp_item, json_item, npy_local_item, txt_local_item

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

        _check((True, True, True, True), (True, True, True, True))
        _check((False, False, False, False), (False, False, False, True))

        with pli.no_data_cache(pli.NumpyItem):
            _check((True, True, True, True), (True, True, True, True))
            _check((False, False, False, False), (False, False, False, True))
            _check((None, None, None, None), (False, False, False, True))

        with pli.no_data_cache(pli.NumpyItem, pli.JsonMetadataItem):
            pli.enable_item_data_cache(pli.ImageItem)
            _check((True, True, True, True), (True, True, True, True))
            _check((False, False, False, False), (False, False, False, True))
            _check((None, None, None, None), (True, False, False, True))
            _check((None, None, None, None), (True, False, False, True))

        with pli.no_data_cache():
            with pli.data_cache(pli.JsonMetadataItem):
                _check((True, True, True, True), (True, True, True, True))
                _check((False, False, False, False), (False, False, False, True))
                _check((None, None, None, None), (False, True, False, True))
                _check((None, None, None, None), (False, True, False, True))

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

        input_seq = SamplesSequence.from_underfolder(
            minimnist_private_dataset["path"], merge_root_items=True
        )

        with pli.item_disabled_serialization_modes(
            [pli.SerializationMode.DEEP_COPY, "HARD_LINK"], pli.ImageItem
        ):
            for _ in input_seq.to_underfolder(folder=(tmp_path / "output_mx")):
                pass
            for p in (tmp_path / "output_mx").rglob("*"):
                if p.is_file():
                    if p.suffix == pli.PngImageItem.file_extensions()[0]:
                        # images should have been created as new files
                        assert p.stat().st_nlink == 1
                    else:
                        # other files should be hard links
                        assert p.stat().st_nlink > 1
