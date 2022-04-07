import imageio
import tifffile
import numpy as np
import typing as t

from pipelime.items.numpy_item import NumpyItem


class ImageItem(NumpyItem):
    """Base class for all image types. Subclasses should implement `file_extensions`
    and, optionally, `save_options`.
    """

    @classmethod
    def save_options(cls) -> t.Mapping[str, t.Any]:
        """Subclass can override and return specific saving options.

        :return: saving parameters
        :rtype: t.Mapping[str, t.Any]
        """
        return {}

    @classmethod
    def decode(cls, fp: t.BinaryIO) -> np.ndarray:
        return np.array(imageio.imread(fp, format=cls.file_extensions()[0]))

    @classmethod
    def encode(cls, value: np.ndarray, fp: t.BinaryIO):
        imageio.imwrite(
            fp, value, format=cls.file_extensions()[0], **cls.save_options()
        )


class BmpImageItem(ImageItem):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".bmp",)


class PngImageItem(ImageItem):
    @classmethod
    def save_options(cls) -> t.Mapping[str, t.Any]:
        return {"compress_level": 4}

    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".png",)


class JpegImageItem(ImageItem):
    @classmethod
    def save_options(cls) -> t.Mapping[str, t.Any]:
        return {"quality ": 80}

    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".jpeg", ".jpg", ".jfif", ".jpe")


class TiffImageItem(ImageItem):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".tiff", ".tif")

    @classmethod
    def save_options(cls) -> t.Mapping[str, t.Any]:
        return {"compression": "zlib"}

    @classmethod
    def decode(cls, fp: t.BinaryIO) -> np.ndarray:
        return np.array(tifffile.imread(fp))

    @classmethod
    def encode(cls, value: np.ndarray, fp: t.BinaryIO):
        tifffile.imwrite(fp, value, **cls.save_options())
