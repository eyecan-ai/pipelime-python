import pytest
from pathlib import Path
import pipelime.sequences as pls

import typing as t


class TestSamplesSequenceWriters:
    def _read_write_data(
        self, source_dataset, out_folder, **writer_kwargs
    ) -> t.Tuple[pls.SamplesSequence, pls.SamplesSequence]:
        source = pls.SamplesSequence.from_underfolder(  # type: ignore
            folder=source_dataset["path"], merge_root_items=True
        )
        for _ in source.to_underfolder(folder=out_folder, **writer_kwargs):
            pass
        dest = pls.SamplesSequence.from_underfolder(  # type: ignore
            folder=out_folder, merge_root_items=True
        )
        return source, dest

    def _check_data(self, source: pls.SamplesSequence, dest: pls.SamplesSequence):
        from pipelime.items.numpy_item import NumpyItem
        import numpy as np

        assert len(source) == len(dest)
        for s1, s2 in zip(source, dest):
            assert s1.keys() == s2.keys()
            for k, v1 in s1.items():
                v2 = s2[k]
                assert v1.__class__ == v2.__class__
                if isinstance(v1, NumpyItem):
                    assert np.array_equal(v1(), v2(), equal_nan=True)  # type: ignore
                else:
                    assert v1() == v2()

    def test_to_underfolder(self, minimnist_dataset: dict, tmp_path: Path):
        source, dest = self._read_write_data(minimnist_dataset, tmp_path / "outfolder")
        self._check_data(source, dest)

    @pytest.mark.parametrize("exists_ok", [True, False])
    def test_to_underfolder_exists(self, tmp_path: Path, exists_ok: bool):
        tmp_path = tmp_path / "outfolder" / "data"
        tmp_path.mkdir(parents=True)

        try:
            _ = pls.SamplesSequence.from_list([]).to_underfolder(  # type: ignore
                folder=tmp_path, exists_ok=exists_ok
            )
        except FileExistsError:
            assert not exists_ok
            return
        assert exists_ok

    def test_to_underfolder_serialization_mode(
        self, minimnist_dataset: dict, tmp_path: Path
    ):
        import pipelime.items as pli

        # previous test runs may already have hard-linked the original files
        # so we need to get a temporary copy first
        src_path = tmp_path / "source"
        with pli.item_serialization_mode(pli.SerializationMode.DEEP_COPY):
            self._read_write_data(minimnist_dataset, src_path)

        item_key = minimnist_dataset["item_keys"][0]
        with pli.item_serialization_mode(pli.SerializationMode.HARD_LINK):
            source, dest = self._read_write_data(
                {"path": src_path},
                tmp_path / "outfolder",
                key_serialization_mode={item_key: pli.SerializationMode.DEEP_COPY},
            )
        self._check_data(source, dest)

        for sample in dest:
            for key, item in sample.items():
                assert isinstance(item._file_sources, t.Sequence)
                assert len(item._file_sources) == 1
                path = Path(item._file_sources[0])
                assert not path.is_symlink()
                assert path.is_file()
                assert path.stat().st_nlink == (1 if key == item_key else 2)

    def test_deep_copy(self, minimnist_dataset: dict, tmp_path: Path):
        import pipelime.items as pli

        with pli.item_serialization_mode(pli.SerializationMode.DEEP_COPY):
            source, dest = self._read_write_data(
                minimnist_dataset, tmp_path / "outfolder"
            )
        self._check_data(source, dest)

        for sample in dest:
            for item in sample.values():
                assert isinstance(item._file_sources, t.Sequence)
                assert len(item._file_sources) == 1
                path = Path(item._file_sources[0])
                assert not path.is_symlink()
                assert path.is_file()
                assert path.stat().st_nlink == 1

    def test_symlink(self, minimnist_dataset: dict, tmp_path: Path):
        import pipelime.items as pli
        import platform

        with pli.item_serialization_mode(pli.SerializationMode.SYM_LINK):
            source, dest = self._read_write_data(
                minimnist_dataset, tmp_path / "outfolder"
            )
        self._check_data(source, dest)

        on_windows = platform.system() == "Windows"
        for sample in dest:
            for item in sample.values():
                assert isinstance(item._file_sources, t.Sequence)
                assert len(item._file_sources) == 1
                path = Path(item._file_sources[0])
                assert on_windows or path.is_symlink()
                assert path.is_file()
                assert path.stat().st_nlink == 1

    def test_hardlink(self, minimnist_dataset: dict, tmp_path: Path):
        import pipelime.items as pli

        # previous test runs may already have hard-linked the original files
        # so we need to get a temporary copy first
        src_path = tmp_path / "source"
        with pli.item_serialization_mode(pli.SerializationMode.DEEP_COPY):
            self._read_write_data(minimnist_dataset, src_path)

        with pli.item_serialization_mode(pli.SerializationMode.HARD_LINK):
            source, dest = self._read_write_data(
                {"path": src_path}, tmp_path / "outfolder"
            )
        self._check_data(source, dest)

        for sample in dest:
            for item in sample.values():
                assert isinstance(item._file_sources, t.Sequence)
                assert len(item._file_sources) == 1
                path = Path(item._file_sources[0])
                assert not path.is_symlink()
                assert path.is_file()
                assert path.stat().st_nlink == 2

    def test_create_new_file(self, minimnist_dataset: dict, tmp_path: Path):
        import pipelime.items as pli

        with pli.item_serialization_mode(pli.SerializationMode.CREATE_NEW_FILE):
            source, dest = self._read_write_data(
                minimnist_dataset, tmp_path / "outfolder"
            )
        self._check_data(source, dest)

        for sample in dest:
            for item in sample.values():
                assert isinstance(item._file_sources, t.Sequence)
                assert len(item._file_sources) == 1
                path = Path(item._file_sources[0])
                assert not path.is_symlink()
                assert path.is_file()
                assert path.stat().st_nlink == 1
