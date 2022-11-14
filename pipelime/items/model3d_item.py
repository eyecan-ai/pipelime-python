import typing as t

from pipelime.items import Item
from pipelime.items.binary_item import BinaryItem


class Model3DItem(BinaryItem):
    pass


class STLModel3DItem(Model3DItem):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".stl",)


class OBJModel3DItem(Model3DItem):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".obj",)
