import imageio.v3 as iio
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
        return np.array(iio.imread(fp, extension=cls.file_extensions()[0]))

    @classmethod
    def encode(cls, value: np.ndarray, fp: t.BinaryIO):
        iio.imwrite(fp, value, extension=cls.file_extensions()[0], **cls.save_options())

    @classmethod
    def pl_pretty_data(cls, value: np.ndarray) -> t.Any:
        IMAGE_SIZE = 32
        if value.shape[0] > IMAGE_SIZE or value.shape[1] > IMAGE_SIZE:
            import albumentations as A

            if value.dtype != np.uint8:
                src_min, src_max = value.min(), value.max()
                value = (value.astype(np.float32) + src_min) / float(src_max - src_min)
                value = (value * 255.0).astype(np.uint8)

            value = A.LongestMaxSize(max_size=IMAGE_SIZE, always_apply=True)(
                image=value
            )["image"]

        def _get_color_str(v) -> str:
            return (
                f"{v[0]},{v[1]},{v[2]}" if isinstance(v, np.ndarray) else f"{v},{v},{v}"
            )

        return "\n".join(
            [
                "".join(
                    [
                        f"[rgb({_get_color_str(value[v, u])})]█[/]"
                        for u in range(value.shape[1])
                    ]
                )
                for v in range(value.shape[0])
            ]
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
