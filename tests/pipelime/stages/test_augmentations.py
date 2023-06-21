import pytest
from pathlib import Path
from pipelime.sequences import Sample
import typing as t


class TestAugmentationStages:
    def _albumentations_transform(self):
        import albumentations as A

        return A.Compose(
            [
                A.Resize(height=50, width=50, interpolation=1, always_apply=False, p=1),
                A.Equalize(mode="cv", by_channels=True, always_apply=False, p=1),
            ]
        )

    def _transformation_dict(self):
        return {
            "__version__": "1.0.3",
            "transform": {
                "__class_fullname__": "Compose",
                "p": 1.0,
                "transforms": [
                    {
                        "__class_fullname__": "Resize",
                        "always_apply": False,
                        "p": 1,
                        "height": 50,
                        "width": 50,
                        "interpolation": 1,
                    },
                    {
                        "__class_fullname__": "Equalize",
                        "always_apply": False,
                        "p": 1,
                        "mode": "cv",
                        "by_channels": True,
                    },
                ],
                "bbox_params": None,
                "keypoint_params": None,
                "additional_targets": {},
            },
        }

    def _load_sample(self, idx: int, folder: Path) -> t.Tuple[Sample, Sample, dict]:
        from pipelime.items import PngImageItem

        items = {}
        items_aug = {}
        keys2trg = {}
        for p in folder.glob(f"{idx}_*_image.png"):
            item_key = p.stem.split("_")[1]
            items[item_key] = PngImageItem(p)
            items_aug[item_key] = PngImageItem(p.parent / f"{p.stem}_aug.png")
            keys2trg[item_key] = "image"
        for p in folder.glob(f"{idx}_*_mask.png"):
            item_key = p.stem.split("_")[1]
            items[item_key] = PngImageItem(p)
            items_aug[item_key] = PngImageItem(p.parent / f"{p.stem}_aug.png")
            keys2trg[item_key] = "mask"
        return Sample(items), Sample(items_aug), keys2trg

    def _stage_albumentations_test_helper(self, folder, transform, outkey, outkey_fm):
        from pipelime.stages import StageAlbumentations
        import numpy as np

        sample, sample_gt, keys2trg = self._load_sample(0, folder)
        stage = StageAlbumentations(
            transform=transform, keys_to_targets=keys2trg, output_key_format=outkey
        )

        sample_aug = stage(sample)
        for key in keys2trg:
            keyaug = outkey_fm.replace("*", key)
            assert key in sample
            assert key in sample_gt
            assert key in sample_aug
            assert keyaug not in sample
            assert keyaug not in sample_gt
            assert keyaug in sample_aug

            assert np.array_equal(
                sample[key](), sample_aug[key](), equal_nan=True  # type: ignore
            )
            assert np.array_equal(
                sample_gt[key](), sample_aug[keyaug](), equal_nan=True  # type: ignore
            )

    def _stage_albumentations_test(self, folder, transform):
        self._stage_albumentations_test_helper(folder, transform, "img-*_", "img-*_")
        self._stage_albumentations_test_helper(folder, transform, "Aug", "*Aug")

    def test_transformation_object(self, augmentations_folder: Path):
        from pipelime.stages.augmentations import Transformation

        dict_tr = self._transformation_dict()
        self._stage_albumentations_test(
            augmentations_folder, Transformation(__root__=dict_tr)
        )

    def test_albumentation_object(self, augmentations_folder: Path):
        tr = self._albumentations_transform()
        self._stage_albumentations_test(augmentations_folder, tr)

    def test_albumentation_json(self, augmentations_folder: Path):
        self._stage_albumentations_test(
            augmentations_folder, augmentations_folder / "albumentations.json"
        )

    def test_albumentation_dict(self, augmentations_folder: Path):
        dict_tr = self._transformation_dict()
        self._stage_albumentations_test(augmentations_folder, dict_tr)

    def test_invalid_transformation(self):
        from pipelime.stages import StageAlbumentations

        with pytest.raises(ValueError):
            _ = StageAlbumentations(
                transform=42,
                keys_to_targets={"image": "image"},
            )


class TestResize:
    @pytest.mark.parametrize(
        "size", [(10, 10), (50, 10), (1, 1), ("max", 20), ("min", 20)]
    )
    @pytest.mark.parametrize("interpolation", ["nearest", "bilinear", "bicubic"])
    def test_stage(self, size, interpolation) -> None:
        from pipelime.stages import StageResize
        import pipelime.items as pli
        import numpy as np

        stage = StageResize(
            size=size,
            interpolation=interpolation,
            images="image",
            masks=["mask1", "mask2"],
            output_key_format="*_resized",
        )

        sample = Sample(
            {
                "image": pli.PngImageItem(
                    np.random.randint(0, 255, (200, 100, 3), dtype=np.uint8)
                ),
                "mask1": pli.PngImageItem(
                    np.random.randint(0, 255, (200, 100), dtype=np.uint8)
                ),
                "mask2": pli.PngImageItem(
                    np.random.randint(0, 255, (200, 100, 10), dtype=np.uint8)
                ),
            }
        )

        sample = stage(sample)

        in_keys = ["image", "mask1", "mask2"]
        out_keys = [f"{k}_resized" for k in in_keys]
        for in_key, out_key in zip(in_keys, out_keys):
            assert in_key in sample
            assert out_key in sample
            orig_image = sample[in_key]()
            resized_image = sample[out_key]()
            assert isinstance(orig_image, np.ndarray)
            assert isinstance(resized_image, np.ndarray)
            if isinstance(size[0], int):
                assert resized_image.shape[:2] == size
            elif size[0] == "max":
                assert max(resized_image.shape[:2]) == size[1]
            else:
                assert min(resized_image.shape[:2]) == size[1]
            if len(resized_image.shape) == 3:
                assert resized_image.shape[2] == orig_image.shape[2]


class TestCropAndPad:
    pass
