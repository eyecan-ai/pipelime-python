import typing as t
import trimesh
from trimesh.parent import Geometry

from pipelime.items import Item
from pipelime.items.base import deferred_classattr


class Model3DItem(Item[Geometry]):
    """Base class for all mesh types. Subclasses should implement `file_extensions`,
    and, optionally, `save_options` and `load_options`.
    """

    @deferred_classattr
    def default_concrete(cls):
        return OBJModel3DItem

    @classmethod
    def save_options(cls) -> t.Mapping[str, t.Any]:
        """Subclass can override and return specific saving options.

        Returns:
          t.Mapping[str, t.Any]: saving parameters
        """
        return {}

    @classmethod
    def load_options(cls) -> t.Mapping[str, t.Any]:
        """Subclass can override and return specific loading options.

        Returns:
          t.Mapping[str, t.Any]: loading parameters
        """
        return {}

    @classmethod
    def decode(cls, fp: t.BinaryIO) -> Geometry:
        return trimesh.load(
            fp, file_type=cls.file_extensions()[0][1:], **cls.load_options()  # type: ignore
        )

    @classmethod
    def encode(cls, value: Geometry, fp: t.BinaryIO):
        value.export(fp, file_type=cls.file_extensions()[0][1:], **cls.save_options())


class STLModel3DItem(Model3DItem):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".stl",)


class OBJModel3DItem(Model3DItem):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".obj",)


class PLYModel3DItem(Model3DItem):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".ply",)


class OFFModel3DItem(Model3DItem):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".off",)


class GLBModel3DItem(Model3DItem):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".glb",)


class GLTFModel3DItem(Model3DItem):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".gltf",)

    @classmethod
    def encode(cls, value: Geometry, fp: t.BinaryIO):
        import base64
        import json
        from io import TextIOWrapper
        from trimesh.exchange import gltf

        def _encode_data(target, source):
            for bin_data in target:
                if "uri" in bin_data and bin_data["uri"] in source:
                    bin_data["uri"] = (
                        "data:application/octet-stream;base64,"
                        + base64.b64encode(source[bin_data["uri"]]).decode()
                    )

        data = gltf.export_gltf(value)
        for k, v in data.items():
            if k.endswith(".gltf"):
                v = json.loads(v)
                if "buffers" in v:
                    _encode_data(v["buffers"], data)
                if "images" in v:
                    _encode_data(v["images"], data)
                json.dump(v, TextIOWrapper(fp))
