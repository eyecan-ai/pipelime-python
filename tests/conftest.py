from pathlib import Path
import os
import pytest


@pytest.fixture(scope="session")
def data_folder() -> Path:
    return Path(__file__).parent / "sample_data"


@pytest.fixture(scope="session")
def datasets_folder(data_folder: Path) -> Path:
    return data_folder / "datasets"


@pytest.fixture(scope="session")
def items_folder(data_folder: Path) -> Path:
    return data_folder / "items"


@pytest.fixture(scope="session")
def augmentations_folder(data_folder: Path) -> Path:
    return data_folder / "augmentations"


@pytest.fixture(scope="session")
def choixe_folder(data_folder: Path) -> Path:
    return data_folder / "choixe"


@pytest.fixture(scope="session")
def minimnist_dataset(datasets_folder: Path) -> dict:
    return {
        "path": datasets_folder / "underfolder_minimnist",
        "root_keys": ["cfg", "numbers", "pose"],
        "item_keys": ["image", "label", "mask", "metadata", "points"],
        "image_keys": ["image", "mask"],
        "len": 20,
    }


@pytest.fixture(scope="function")
def minimnist_private_dataset(minimnist_dataset: dict, tmp_path: Path) -> dict:
    from shutil import copytree

    dest = Path(
        copytree(str(minimnist_dataset["path"]), str(tmp_path / "minimnist_private"))
    )
    minimnist_dataset["path"] = dest
    return minimnist_dataset


@pytest.fixture(scope="function")
def minio(tmp_path: Path):
    minio_path = os.environ.get("MINIO_APP")
    if not minio_path or not Path(minio_path).is_file():
        yield ""
        return

    try:
        import minio  # noqa
    except ModuleNotFoundError:
        yield ""
        return

    from subprocess import Popen
    from time import sleep

    minio_root = tmp_path / ".minio"
    minio_root.mkdir()
    minio_proc = Popen([minio_path, "server", str(minio_root)])
    sleep(5)
    yield "minioadmin"

    # teardown
    minio_proc.terminate()
