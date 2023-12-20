class TestGeneralCommandsBase:
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
        "values": {
            "class_path": "YamlMetadataItem",
            "is_optional": False,
            "is_shared": False,
        },
        "points": {
            "class_path": "TxtNumpyItem",
            "is_optional": False,
            "is_shared": False,
        },
    }
