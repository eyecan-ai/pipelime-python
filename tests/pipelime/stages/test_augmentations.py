from pathlib import Path
from pipelime.sequences import Sample
import typing as t


class TestAugmentationStages:
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

    def _stage_albumentations_test(self, folder, transform):
        from pipelime.stages import StageAlbumentations
        import numpy as np

        sample, sample_gt, keys2trg = self._load_sample(0, folder)
        stage = StageAlbumentations(
            transform, keys_to_targets=keys2trg, output_key_format="*_aug"
        )

        sample_aug = stage(sample)
        for key in keys2trg:
            keyaug = key + "_aug"
            assert key in sample
            assert key in sample_gt
            assert key in sample_aug
            assert keyaug not in sample
            assert keyaug not in sample_gt
            assert keyaug in sample_aug

            assert np.all(sample[key]() == sample_aug[key]())
            assert np.all(sample_gt[key]() == sample_aug[keyaug]())

    def test_albumentation_object(self, augmentations_folder: Path):
        import albumentations as A

        tr = A.Compose(
            [
                A.Resize(
                    height=50, width=50, interpolation=1, always_apply=False, p=1.0
                ),
                A.Equalize(mode="cv", by_channels=True, always_apply=False, p=1.0),
            ]
        )
        self._stage_albumentations_test(augmentations_folder, tr)

    def test_albumentation_json(self, augmentations_folder: Path):
        self._stage_albumentations_test(
            augmentations_folder, augmentations_folder / "albumentations.json"
        )

    def test_albumentation_dict(self, augmentations_folder: Path):
        dict_tr = {
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
        self._stage_albumentations_test(augmentations_folder, dict_tr)
