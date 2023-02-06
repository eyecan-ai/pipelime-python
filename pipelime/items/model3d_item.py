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
