import pytest
import typing as t


class TestGeneralCommands:
    minimnist_partial_schema = {
        "pose": {
            "class_path": "TxtNumpyItem",
            "is_optional": False,
            "is_shared": True,
        },
        "image": {
            "class_path": "PngImageItem",
            "is_optional": False,
            "is_shared": False,
        },
    }
    minimnist_full_schema = {
        "cfg": {
            "class_path": "YamlMetadataItem",
            "is_optional": False,
            "is_shared": True,
        },
        "numbers": {
            "class_path": "NumpyItem",
            "is_optional": False,
            "is_shared": True,
        },
        "pose": {
            "class_path": "TxtNumpyItem",
            "is_optional": False,
            "is_shared": True,
        },
        "image": {
            "class_path": "ImageItem",
            "is_optional": False,
            "is_shared": False,
        },
        "label": {
            "class_path": "TxtNumpyItem",
            "is_optional": False,
            "is_shared": False,
        },
        "mask": {
            "class_path": "ImageItem",
            "is_optional": False,
            "is_shared": False,
        },
        "metadata": {
            "class_path": "MetadataItem",
            "is_optional": False,
            "is_shared": False,
        },
        "points": {
            "class_path": "TxtNumpyItem",
            "is_optional": False,
            "is_shared": False,
        },
    }

    @pytest.mark.parametrize("lazy", [True, False])
    @pytest.mark.parametrize("ignore_extra_keys", [True, False])
    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [1, 2, 4])
    def test_clone(
        self,
        minimnist_dataset,
        lazy: bool,
        ignore_extra_keys: bool,
        nproc: int,
        prefetch: int,
        tmp_path,
    ):
        from pipelime.commands import CloneCommand

        cmd = CloneCommand.parse_obj(
            {
                "input": {
                    "folder": minimnist_dataset["path"],
                    "schema": {
                        "sample_schema": TestGeneralCommands.minimnist_partial_schema
                        if ignore_extra_keys
                        else TestGeneralCommands.minimnist_full_schema,
                        "ignore_extra_keys": ignore_extra_keys,
                        "lazy": lazy,
                    },
                },
                "output": {
                    "folder": tmp_path / "output",
                    "serialization": {
                        "override": {"DEEP_COPY": None},
                        "disable": {"MetadataItem": ["HARD_LINK", "DEEP_COPY"]},
                        "keys": {"image": "HARD_LINK"},
                    },
                },
                "grabber": f"{nproc},{prefetch}",
            }
        )
        cmd()
