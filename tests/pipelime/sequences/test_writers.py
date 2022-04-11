import pytest
from pathlib import Path
import pipelime.sequences as pls


class TestSamplesSequenceWriters:
    def test_to_underfolder(self, minimnist_dataset: dict, tmp_path: Path):
        from pipelime.items.numpy_item import NumpyItem
        import numpy as np

        tmp_path = tmp_path / "outfolder"

        source = pls.SamplesSequence.from_underfolder(  # type: ignore
            folder=minimnist_dataset["path"], merge_root_items=True
        )

        for s in source.to_underfolder(folder=tmp_path):
            pass

        dest = pls.SamplesSequence.from_underfolder(  # type: ignore
            folder=tmp_path, merge_root_items=True
        )
        for s1, s2 in zip(source, dest):
            assert len(s1.keys() ^ s2.keys()) == 0
            for k, v1 in s1.items():
                v2 = s2[k]
                assert v1.__class__ == v2.__class__
                if isinstance(v1, NumpyItem):
                    assert np.all(v1() == v2())
                else:
                    assert v1() == v2()

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
