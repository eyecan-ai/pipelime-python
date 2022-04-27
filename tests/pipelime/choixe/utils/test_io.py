from pathlib import Path

import pytest
from pipelime.choixe.utils.io import dump, load

data = {
    "alice": 10,
    "bob": 20,
    "charlie": ["foo", "bar", {"alpha": 10.0, "beta": 20.0}],
}


@pytest.mark.parametrize(["ext"], [["yml"], ["yaml"], ["json"]])
def test_io(tmp_path: Path, ext: str):
    path = tmp_path / f"config.{ext}"
    dump(data, path)
    loaded = load(path)
    assert loaded == data
