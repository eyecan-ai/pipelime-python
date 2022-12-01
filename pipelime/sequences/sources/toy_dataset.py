import typing as t
import numpy as np

import pydantic as pyd

import pipelime.sequences as pls


@pls.source_sequence
class ToyDataset(
    pls.SamplesSequence, title="toy_dataset", arbitrary_types_allowed=True
):
    """A fake sequence of generated samples."""

    length: pyd.PositiveInt = pyd.Field(
        ..., description="The number of samples to generate."
    )
    with_images: bool = pyd.Field(True, description="Whether to generate images.")
    with_masks: bool = pyd.Field(
        True, description="Whether to generate masks with object labels."
    )
    with_instances: bool = pyd.Field(
        True, description="Whether to generate images with object indexes."
    )
    with_objects: bool = pyd.Field(True, description="Whether to generate objects.")
    with_bboxes: bool = pyd.Field(
        True, description="Whether to generate objects' bboxes."
    )
    with_kpts: bool = pyd.Field(
        True, description="Whether to generate objects' keypoints."
    )
    image_size: t.Union[
        pyd.PositiveInt, t.Tuple[pyd.PositiveInt, pyd.PositiveInt]
    ] = pyd.Field(256, description="The size of the generated images.")
    key_format: str = pyd.Field(
        "*",
        description=(
            "The sample key format. Any `*` will be replaced with the "
            "base key name, eg, `my_*_key` on [`image`, `mask`] generates "
            "`my_image_key` and `my_mask_key`. If no `*` is found, the string is "
            "suffixed to the base key name, ie, `MyKey` on `image` gives "
            "`imageMyKey`. If empty, the base key name will be used as-is."
        ),
    )
    max_labels: pyd.NonNegativeInt = pyd.Field(
        5, description="The maximum number assigned to object labels in the dataset."
    )
    objects_range: t.Tuple[pyd.NonNegativeInt, pyd.NonNegativeInt] = pyd.Field(
        (1, 5), description="The (min, max) number of objects in each sample."
    )

    seed: t.Optional[int] = pyd.Field(None, description="The optional random seed.")

    _sample_cache: t.Dict[int, pls.Sample] = pyd.PrivateAttr(default_factory=dict)
    _rnd_gen: np.random.Generator = pyd.PrivateAttr(None)

    @pyd.validator("key_format")
    def validate_key_format(cls, v):
        if "*" in v:
            return v
        return "*" + v

    def __init__(self, length: int, **data):
        super().__init__(length=length, **data)  # type: ignore

    def size(self) -> int:
        return self.length

    def image_shape(self) -> t.Tuple[int, int]:
        if isinstance(self.image_size, int):
            return (self.image_size, self.image_size)
        return self.image_size

    def get_sample(self, idx: int) -> pls.Sample:
        if idx < 0 or idx >= self.size():
            raise IndexError(f"Sample index `{idx}` is out of range.")
        if idx in self._sample_cache:
            return self._sample_cache[idx]

        # create the random generator here, so that processes do not share the seed
        if self._rnd_gen is None:
            self._rnd_gen = np.random.default_rng(self.seed)

        sample = self._generate_sample(idx)
        self._sample_cache[idx] = sample
        return sample

    def _generate_sample(self, idx) -> pls.Sample:
        import uuid
        import pipelime.items as pli
        import json

        label_key = self.key_format.replace("*", "label")
        bboxes_key = self.key_format.replace("*", "bboxes")
        kpts_key = self.key_format.replace("*", "keypoints")

        metadata = {
            label_key: int(self._rnd_gen.integers(self.max_labels + 1)),
            self.key_format.replace("*", "id"): idx
            + (
                self.seed
                if self.seed is not None
                else int(self._rnd_gen.integers(self.length))
            ),
        }

        objects = []
        if self.with_objects:
            no_objs = self._rnd_gen.integers(
                self.objects_range[0], self.objects_range[1]
            )
            objects = [self._generate_2d_object() for _ in range(no_objs)]
            if self.with_bboxes:
                metadata[bboxes_key] = [self._generate_bbox(obj) for obj in objects]
            if self.with_kpts:
                metadata[kpts_key] = [self._generate_kpts(obj) for obj in objects]

        items: t.Dict[str, pli.Item] = {
            k: pli.PngImageItem(v) for k, v in self._generate_images(objects).items()
        }
        items[self.key_format.replace("*", "metadata")] = pli.YamlMetadataItem(
            json.loads(json.dumps(metadata))
        )
        items[label_key] = pli.TxtNumpyItem(metadata[label_key])
        if self.with_objects and self.with_bboxes:
            items[bboxes_key] = pli.NpyNumpyItem(metadata[bboxes_key])
        if self.with_objects and self.with_kpts:
            items[kpts_key] = pli.PickleItem(metadata[kpts_key])

        return pls.Sample(items)

    def _generate_2d_object(self):
        size = np.array(self.image_shape())
        center = self._rnd_gen.uniform(0.25 * size[0], 0.75 * size[0], (2,))
        random_size = self._rnd_gen.uniform(size * 0.05, size * 0.24, (2,))
        top_left = np.array(
            [center[0] - random_size[0] * 0.5, center[1] - random_size[1] * 0.5]
        )
        bottom_right = np.array(
            [center[0] + random_size[0] * 0.5, center[1] + random_size[1] * 0.5]
        )
        diag = bottom_right - top_left  # type: ignore

        kp0 = np.array([center[0], center[1] - random_size[1] * 0.5])
        kp1 = np.array([center[0], center[1] + random_size[1] * 0.5])

        width = random_size[0]
        height = random_size[1]
        label = int(self._rnd_gen.integers(0, self.max_labels + 1))

        return {
            "center": center,
            "size": random_size,
            "tl": top_left,
            "br": bottom_right,
            "diag": diag,
            "kp0": kp0,
            "kp1": kp1,
            "w": width,
            "h": height,
            "label": label,
        }

    def _generate_bbox(self, obj):
        center = obj["center"]
        return (
            center[0] - obj["w"] * 0.5,
            center[1] - obj["h"] * 0.5,
            center[0] + obj["w"] * 0.5,
            center[1] + obj["h"] * 0.5,
            obj["label"],
        )

    def _generate_kpts(self, obj):
        tl = obj["tl"]
        br = obj["br"]
        diag = obj["diag"]
        label = obj["label"]

        orientation = np.arctan2(diag[1], diag[0])
        orientation2 = np.arctan2(-diag[1], -diag[0])

        scale = 0.5 * np.linalg.norm(diag)

        kp1 = (tl[0], tl[1], -orientation, scale, label, 1)
        kp2 = (br[0], br[1], -orientation2, scale, label, 2)

        return [kp1, kp2]

    def _generate_images(self, objs):
        from PIL import Image, ImageDraw

        image = (
            Image.fromarray(
                self._rnd_gen.integers(
                    0, 256, self.image_shape() + (3,), dtype=np.uint8
                )
            )
            if self.with_images
            else None
        )
        mask = Image.new("L", self.image_shape()) if self.with_masks else None
        instances = Image.new("L", self.image_shape()) if self.with_instances else None

        for index, obj in enumerate(objs):
            label = obj["label"]
            coords = tuple(obj["tl"]), tuple(obj["br"])
            color = tuple(self._rnd_gen.integers(0, 255, (3,)))

            if image:
                ImageDraw.Draw(image).rectangle(coords, fill=color)
            if mask:
                ImageDraw.Draw(mask).rectangle(coords, fill=label + 1)
            if instances:
                ImageDraw.Draw(instances).rectangle(coords, fill=index + 1)

        image_map = {}
        if image:
            image_map[self.key_format.replace("*", "image")] = np.array(image)
        if mask:
            image_map[self.key_format.replace("*", "mask")] = np.array(mask)
        if instances:
            image_map[self.key_format.replace("*", "instances")] = np.array(instances)
        return image_map
