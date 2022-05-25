import numpy as np
import warnings
import typing as t

from pipelime.items import Item


class NumpyItem(Item[np.ndarray]):
    @classmethod
    def validate(cls, raw_data: t.Any) -> np.ndarray:
        return np.array(raw_data)


class NpyNumpyItem(NumpyItem):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".npy",)

    @classmethod
    def decode(cls, fp: t.BinaryIO) -> np.ndarray:
        return np.load(fp)

    @classmethod
    def encode(cls, value: np.ndarray, fp: t.BinaryIO):
        np.save(fp, value)


class TxtNumpyItem(NumpyItem):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".txt",)

    @classmethod
    def decode(cls, fp: t.BinaryIO) -> np.ndarray:
        with warnings.catch_warnings():
            warnings.filterwarnings(action="ignore", message="loadtxt")
            return np.atleast_1d(np.loadtxt(fp))

    @classmethod
    def encode(cls, value: np.ndarray, fp: t.BinaryIO):
        np.savetxt(fp, value)

    @classmethod
    def validate(cls, raw_data: t.Any) -> np.ndarray:
        return np.atleast_1d(np.array(raw_data))
