from pathlib import Path
import os
import pytest


@pytest.fixture(scope="session")
def data_folder() -> Path:
    return Path(__file__).parent / "sample_data"


@pytest.fixture(scope="session")
def datasets_folder(data_folder) -> Path:
    return data_folder / "datasets"


@pytest.fixture(scope="session")
def items_folder(data_folder) -> Path:
    return data_folder / "items"


@pytest.fixture(scope="session")
def augmentations_folder(data_folder) -> Path:
    return data_folder / "augmentations"


@pytest.fixture(scope="session")
def minimnist_dataset(datasets_folder) -> dict:
    return {
        "path": datasets_folder / "underfolder_minimnist",
        "root_keys": ["cfg", "numbers", "pose"],
        "item_keys": ["image", "label", "mask", "metadata", "points"],
        "len": 20,
    }


@pytest.fixture(scope="session")
def minio(tmp_path_factory):
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

    minio_root = tmp_path_factory.mktemp(".minio")
    minio_proc = Popen([minio_path, "server", str(minio_root)])
    sleep(5)
    yield "minioadmin"

    # teardown
    minio_proc.terminate()
