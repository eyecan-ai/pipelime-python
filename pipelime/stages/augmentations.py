import typing as t
from pathlib import Path

import albumentations as A
import numpy as np
import pydantic as pyd
from pydantic.color import Color

from pipelime.stages import SampleStage

if t.TYPE_CHECKING:
    from pipelime.sequences import Sample


class Transformation(pyd.BaseModel, extra="forbid", copy_on_model_validation="none"):
    """The albumentations transformation defined as python object,
    serialized dict or yaml/json file.
    """

    __root__: t.Dict[str, t.Any]
    _value: t.Union[A.BaseCompose, A.BasicTransform] = pyd.PrivateAttr(None)

    def __init__(self, **data):
        super().__init__(**data)
        self._value = A.from_dict(self.__root__)  # type: ignore

    @property
    def value(self):
        return self._value

    def __str__(self) -> str:
        return str(self.__root__)

    def __repr__(self) -> str:
        return repr(self.__root__)

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value):
        if isinstance(value, Transformation):
            return value
        if isinstance(value, (A.BaseCompose, A.BasicTransform)):
            return Transformation(__root__=A.to_dict(value))
        if isinstance(value, (str, Path)):
            import yaml

            with open(str(value)) as f:
                value = yaml.safe_load(f)
        if isinstance(value, t.Mapping):
            return Transformation(__root__=value)
        raise ValueError(f"{value} is not a valid transformation")


class StageAlbumentations(SampleStage, title="albumentations"):
    """Sample augmentation via Albumentations."""

    transform: Transformation = pyd.Field(...)
    keys_to_targets: t.Mapping[str, str] = pyd.Field(
        ...,
        description=(
            "A mapping from key names to albumentation targets' names. "
            "NB: key names will be replaced with the target name possibly followed "
            "by an index, ie, `image`, `image_0`, `image_1` etc, "
            "`mask`, `mask_0`, `mask_1` etc."
        ),
    )
    output_key_format: str = pyd.Field(
        "*",
        description=(
            "How to format the output keys. Any `*` will be replaced with the "
            "source key, eg, `aug_*_out` on [`image`, `mask`] generates "
            "`aug_image_out` and `aug_mask_out`. If no `*` is found, the string is "
            "suffixed to the source key, ie, `OutName` on `image` gives "
            "`imageOutName`. If empty, the source key will be used as-is."
        ),
    )

    _target_to_keys: t.Dict[str, str] = pyd.PrivateAttr(default_factory=dict)

    @pyd.validator("output_key_format")
    def validate_output_key_format(cls, v):
        if "*" in v:
            return v
        return "*" + v

    def __init__(self, **data):
        super().__init__(**data)

        target_counter: t.Mapping[str, int] = {}
        target_types: t.Mapping[str, str] = {}

        for key, trg in self.keys_to_targets.items():
            counter = target_counter.get(trg, 0)
            trg_name = trg if counter == 0 else f"{trg}_{counter}"
            target_counter[trg] = counter + 1
            target_types[trg_name] = trg
            self._target_to_keys[trg_name] = key

        self.transform.value.add_targets(target_types)

    def __call__(self, x: "Sample") -> "Sample":
        to_transform = {k: x[v]() for k, v in self._target_to_keys.items() if v in x}
        transformed = self.transform.value(**to_transform)  # type: ignore
        for k, v in transformed.items():
            x_key = self._target_to_keys[k]
            x = x.set_value_as(self.output_key_format.replace("*", x_key), x_key, v)
        return x


class StageResize(SampleStage, title="resize"):
    """Helper stage to resize images and masks without having to define a
    full albumentations transformation.
    """

    size: t.Union[
        t.Tuple[t.Literal["max"], int],
        t.Tuple[t.Literal["min"], int],
        t.Tuple[int, int],
    ] = pyd.Field(..., description=("The target size."))
    interpolation: t.Literal["nearest", "bilinear", "bicubic"] = pyd.Field(
        "bilinear", description=("The interpolation method to use.")
    )
    images: t.Union[str, t.Sequence[str]] = pyd.Field(
        [], description=("A list of image keys to resize.")
    )
    masks: t.Union[str, t.Sequence[str]] = pyd.Field(
        [],
        description=(
            "A list of mask keys to resize. No interpolation is used, regardless "
            "of the `interpolation` parameter."
        ),
    )
    output_key_format: str = pyd.Field(
        "*", description=("How to format the output keys.")
    )

    _wrapped: StageAlbumentations = pyd.PrivateAttr()

    def __init__(self, **data) -> None:
        import cv2

        super().__init__(**data)

        self.images = [self.images] if isinstance(self.images, str) else self.images
        self.masks = [self.masks] if isinstance(self.masks, str) else self.masks

        # Create the albumentations transform
        interp_map = {
            "nearest": cv2.INTER_NEAREST,
            "bilinear": cv2.INTER_LINEAR,
            "bicubic": cv2.INTER_CUBIC,
        }
        interp = interp_map[self.interpolation]
        if self.size[0] == "max":
            resize_tr = A.LongestMaxSize(max_size=self.size[1], interpolation=interp)
        elif self.size[0] == "min":
            resize_tr = A.SmallestMaxSize(max_size=self.size[1], interpolation=interp)
        else:
            resize_tr = A.Resize(*self.size, interpolation=interp)
        transforms = A.Compose([resize_tr])

        # Keys to targets
        keys_to_targets = {k: "image" for k in self.images}
        keys_to_targets.update({k: "mask" for k in self.masks})

        # Create the inner stage
        self._wrapped = StageAlbumentations(
            transform=transforms,
            keys_to_targets=keys_to_targets,
            output_key_format=self.output_key_format,
        )

    def __call__(self, x: "Sample") -> "Sample":
        return self._wrapped(x)


class StageCropAndPad(SampleStage, title="crop-and-pad"):
    """Helper stage to crop and pad images in a desired size without having to define
    a full albumentations transformation."""

    x: int = pyd.Field(
        0,
        description=(
            "If positive image is cropped from the left, otherwise image is padded."
        ),
    )
    y: int = pyd.Field(
        0,
        description=(
            "If positive image is cropped from the top, otherwise image is padded."
        ),
    )
    width: pyd.NonNegativeInt = pyd.Field(
        0,
        description=(
            "Width of the output image, cropped or padded from the right as needed. "
            "If 0 no cropping or padding is done."
        ),
    )
    height: pyd.NonNegativeInt = pyd.Field(
        0,
        description=(
            "Height of the output image, cropped or padded from the bottom as needed. "
            "If 0 no cropping or padding is done."
        ),
    )
    border: t.Literal["constant", "reflect", "replicate", "circular"] = pyd.Field(
        "constant", description="Padding mode."
    )
    pad_colors: t.Union[Color, t.Sequence[Color]] = pyd.Field(
        Color("black"), description="Padding color for each image."
    )

    images: t.Union[str, t.Sequence[str]] = pyd.Field(
        "image", description="Keys of the images to crop/pad."
    )
    output_key_format: str = pyd.Field(
        "*", description="How to format the output keys."
    )

    def __call__(self, x: "Sample") -> "Sample":
        import cv2

        img_keys = [self.images] if isinstance(self.images, str) else self.images
        out_keys = [self.output_key_format.replace("*", k) for k in img_keys]
        colors = (
            [self.pad_colors] if isinstance(self.pad_colors, Color) else self.pad_colors
        )

        if len(colors) < len(img_keys):
            colors = list(colors) + [Color("black")] * (len(img_keys) - len(colors))

        for inkey, outkey, pad_col in zip(img_keys, out_keys, colors):
            if inkey in x:
                image: np.ndarray = x[inkey]()  # type: ignore

                from_right = self.width and (self.x + self.width - image.shape[1])
                from_bottom = self.height and (self.y + self.height - image.shape[0])

                cv_border = {
                    "constant": cv2.BORDER_CONSTANT,
                    "reflect": cv2.BORDER_REFLECT_101,
                    "replicate": cv2.BORDER_REPLICATE,
                    "circular": cv2.BORDER_WRAP,
                }

                crop_left = max(0, self.x)
                crop_top = max(0, self.y)
                crop_right = abs(min(0, from_right))
                crop_bottom = abs(min(0, from_bottom))

                outimg = image[
                    crop_top : max(0, image.shape[0] - crop_bottom),
                    crop_left : max(0, image.shape[1] - crop_right),
                ]

                pad_left = (
                    abs(min(0, self.x)) if crop_right < image.shape[1] else self.width
                )
                pad_top = (
                    abs(min(0, self.y)) if crop_bottom < image.shape[0] else self.height
                )
                pad_right = (
                    max(0, from_right) if crop_left < image.shape[1] else self.width
                )
                pad_bottom = (
                    max(0, from_bottom) if crop_top < image.shape[0] else self.height
                )

                outimg = cv2.copyMakeBorder(
                    src=outimg.copy(),
                    top=pad_top,
                    bottom=pad_bottom,
                    left=pad_left,
                    right=pad_right,
                    borderType=cv_border[self.border],
                    value=pad_col.as_rgb_tuple(alpha=False),
                )

                x = x.set_value_as(outkey, inkey, outimg)

        return x
