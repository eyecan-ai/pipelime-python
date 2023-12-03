import os
import typing as t
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def tests_folder() -> Path:
    return Path(__file__).parent.resolve().absolute()


@pytest.fixture(scope="session")
def data_folder(tests_folder: Path) -> Path:
    return tests_folder / "sample_data"


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
def ckpt_dag(data_folder: Path) -> Path:
    return data_folder / "cli" / "ckpt_dag.py"


@pytest.fixture(scope="session")
def shell_cmd(data_folder: Path) -> Path:
    return data_folder / "cli" / "shell_cmd.py"


@pytest.fixture(scope="session")
def minimnist_dataset(datasets_folder: Path) -> dict:
    return {
        "path": datasets_folder / "underfolder_minimnist",
        "root_keys": ["cfg", "numbers", "pose"],
        "item_keys": ["image", "label", "mask", "metadata", "points"],
        "item_types": {
            "cfg": "YamlMetadataItem",
            "numbers": "TxtNumpyItem",
            "pose": "TxtNumpyItem",
            "image": "PngImageItem",
            "label": "TxtNumpyItem",
            "mask": "PngImageItem",
            "metadata": "JsonMetadataItem",
            "points": "TxtNumpyItem",
        },
        "image_keys": ["image", "mask"],
        "len": 20,
    }


@pytest.fixture(scope="session")
def raw_images(datasets_folder: Path) -> Path:
    return datasets_folder / "raw_images"


@pytest.fixture(scope="function")
def minimnist_private_dataset(minimnist_dataset: dict, tmp_path: Path) -> dict:
    from copy import deepcopy
    from shutil import copytree

    minimnist_private = deepcopy(minimnist_dataset)
    dest = Path(
        copytree(str(minimnist_private["path"]), str(tmp_path / "minimnist_private"))
    )
    minimnist_private["path"] = dest
    return minimnist_private


@pytest.fixture(scope="session")
def choixe_plain_cfg(choixe_folder: Path) -> Path:
    return choixe_folder / "plain_cfg.yml"


@pytest.fixture(scope="function")
def all_dags(piper_folder: Path) -> t.Sequence[t.Mapping[str, t.Any]]:
    from . import TestUtils

    def _add_if_exists(out, path, key):
        if path.exists():
            out[key] = path

    all_dags = []
    with os.scandir(str(piper_folder / "dags")) as dirit:
        for entry in dirit:
            if entry.is_dir():
                cfg_path = Path(entry.path) / "dag.yml"
                ctx_path = Path(entry.path) / "ctx.yml"

                dag = {}
                dag["cfg_path"] = cfg_path
                dag["ctx_path"] = ctx_path
                dag["config"] = TestUtils.choixe_process(cfg_path, ctx_path)
                _add_if_exists(dag, Path(entry.path) / "dot.yml", "dot")

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
