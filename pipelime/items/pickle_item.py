import pickle
import typing as t

from pipelime.items import Item


class PickleItem(Item[t.Any]):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return ('.pkl', '.pickle')

    @classmethod
    def decode(cls, fp: t.BinaryIO) -> t.Any:
        return pickle.load(fp)

    @classmethod
    def encode(cls, value: t.Any, fp: t.BinaryIO):
        pickle.dump(value, fp)
