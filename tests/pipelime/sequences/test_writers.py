import pytest
from pathlib import Path
import pipelime.sequences as pls

import typing as t


class TestSamplesSequenceWriters:
    def _read_write_data(
        self, source_dataset, out_folder, **writer_kwargs
    ) -> t.Tuple[pls.SamplesSequence, pls.SamplesSequence]:
        source = pls.SamplesSequence.from_underfolder(
            folder=source_dataset["path"], merge_root_items=True
        )
        for _ in source.to_underfolder(folder=out_folder, **writer_kwargs):
            pass
        dest = pls.SamplesSequence.from_underfolder(
            folder=out_folder, merge_root_items=True
        )
        return source, dest

    def _check_data(self, source: pls.SamplesSequence, dest: pls.SamplesSequence):
        import pipelime.items as pli
        import numpy as np

        assert len(source) == len(dest)
        for s1, s2 in zip(source, dest):
            assert s1.keys() == s2.keys()
            for k, v1 in s1.items():
                v2 = s2[k]
                assert v1.__class__ == v2.__class__
                if isinstance(v1, pli.NumpyItem):
                    assert np.array_equal(v1(), v2(), equal_nan=True)  # type: ignore
                else:
                    assert v1() == v2()

    def _check_data_and_outputs(
        self,
        source: pls.SamplesSequence,
        dest: pls.SamplesSequence,
        nlink_fn: t.Callable[[str], int],
    ):
        self._check_data(source, dest)

        for sample in dest:
            for key, item in sample.items():
                item_sources = item.local_sources

                assert isinstance(item_sources, t.Sequence)
                assert len(item_sources) == 1
                path = Path(item_sources[0])
                assert path.is_file()
                assert not path.is_symlink()
                assert path.stat().st_nlink == nlink_fn(key)

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
        self, minimnist_private_dataset: dict, tmp_path: Path
    ):
        import pipelime.items as pli

        item_key = minimnist_private_dataset["item_keys"][0]
        with pli.item_serialization_mode(pli.SerializationMode.HARD_LINK):
            source, dest = self._read_write_data(
                minimnist_private_dataset,
                tmp_path / "outfolder",
                key_serialization_mode={item_key: pli.SerializationMode.DEEP_COPY},
            )
        self._check_data_and_outputs(
            source, dest, lambda key: 1 if key == item_key else 2
        )

    def test_deep_copy(self, minimnist_dataset: dict, tmp_path: Path):
        import pipelime.items as pli

        with pli.item_serialization_mode(pli.SerializationMode.DEEP_COPY):
            source, dest = self._read_write_data(
                minimnist_dataset, tmp_path / "outfolder"
            )
        self._check_data_and_outputs(source, dest, lambda key: 1)

    def test_symlink(self, minimnist_dataset: dict, tmp_path: Path):
        import pipelime.items as pli
        import platform

        with pli.item_serialization_mode(pli.SerializationMode.SYM_LINK):
            source, dest = self._read_write_data(
                minimnist_dataset, tmp_path / "outfolder"
            )
        self._check_data_and_outputs(source, dest, lambda key: 1)

        if not platform.system() == "Windows":
            for path in (tmp_path / "outfolder").rglob("*"):
                assert not path.is_file() or path.is_symlink()

    def test_hardlink(self, minimnist_private_dataset: dict, tmp_path: Path):
        import pipelime.items as pli

        with pli.item_serialization_mode(pli.SerializationMode.HARD_LINK):
            source, dest = self._read_write_data(
                minimnist_private_dataset, tmp_path / "outfolder"
            )
        self._check_data_and_outputs(source, dest, lambda key: 2)

    def test_create_new_file(self, minimnist_dataset: dict, tmp_path: Path):
        import pipelime.items as pli

        with pli.item_serialization_mode(
            pli.SerializationMode.CREATE_NEW_FILE, pli.MetadataItem, pli.NumpyItem
        ):
            source, dest = self._read_write_data(
                minimnist_dataset, tmp_path / "outfolder"
            )
        self._check_data_and_outputs(source, dest, lambda key: 1)
