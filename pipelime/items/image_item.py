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
        import warnings

        with warnings.catch_warnings():
            warnings.filterwarnings(action="ignore", message=".*format_hint.*")
            return np.array(iio.imread(fp, format_hint=cls.file_extensions()[0]))

    @classmethod
    def encode(cls, value: np.ndarray, fp: t.BinaryIO):
        import warnings

        with warnings.catch_warnings():
            warnings.filterwarnings(action="ignore", message=".*format_hint.*")
            iio.imwrite(
                fp, value, format_hint=cls.file_extensions()[0], **cls.save_options()
            )

    @classmethod
    def pl_pretty_data(cls, value: np.ndarray) -> t.Any:
        IMAGE_SIZE = 32
        if value.shape[0] > IMAGE_SIZE or value.shape[1] > IMAGE_SIZE:
            from PIL import Image

            h, w = value.shape[0], value.shape[1]
            if h > w:
                w = int(w * IMAGE_SIZE / h)
                h = IMAGE_SIZE
            else:
                h = int(h * IMAGE_SIZE / w)
                w = IMAGE_SIZE

            value = np.asarray(
                Image.fromarray(value).resize(
                    (w, h), resample=Image.Resampling.BILINEAR  # type: ignore
                )
            )

        def _get_color_str(v) -> str:
            return (
                f"{v[0]},{v[1]},{v[2]}" if isinstance(v, np.ndarray) else f"{v},{v},{v}"
            )

        return "\n".join(
            [
                "".join(
                    [
                        f"[rgb({_get_color_str(value[v, u])})]\u2588[/]"
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
