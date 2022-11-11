import os
import typing as t
from pathlib import Path

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
def piper_folder(data_folder: Path) -> Path:
    return data_folder / "piper"


@pytest.fixture(scope="session")
def extra_modules(data_folder: Path) -> t.List[t.Dict[str, t.Any]]:
    return [
        {
            "filepath": (data_folder / "cli" / "extra_commands.py"),
            "operators": [],
            "commands": ["randrange"],
        },
        {
            "filepath": (data_folder / "cli" / "extra_operators.py"),
            "operators": ["reversed"],
            "commands": [],
        },
    ]


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


@pytest.fixture(scope="session")
def choixe_plain_cfg(choixe_folder: Path) -> Path:
    return choixe_folder / "plain_cfg.yml"


@pytest.fixture(scope="session")
def all_dags(piper_folder: Path) -> t.Sequence[t.Mapping[str, t.Any]]:
    from pipelime.choixe import XConfig
    import pipelime.choixe.utils.io as choixe_io

    def _add_if_exists(out, path, key):
        if path.exists():
            out[key] = path

    all_dags = []
    with os.scandir(str(piper_folder / "dags")) as dirit:
        for entry in dirit:
            if entry.is_dir():
                cfg = XConfig(choixe_io.load(Path(entry.path) / "dag.yml"))

                ctx_path = Path(entry.path) / "ctx.yml"
                ctx = XConfig(choixe_io.load(ctx_path)) if ctx_path.exists() else None
                cfg = cfg.process(ctx).to_dict()

                dag = {}
                dag["config"] = cfg
                _add_if_exists(dag, Path(entry.path) / "dag.dot", "dot")

                all_dags.append(dag)
    return all_dags


@pytest.fixture(scope="function")
def minio(tmp_path: Path):
    minio_path = os.environ.get("MINIO_APP")
    if not minio_path or not Path(minio_path).is_file():
        yield ""
        return

    try:
        import minio  # noqa: F401
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
