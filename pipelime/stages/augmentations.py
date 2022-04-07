import albumentations as A
import typing as t

from pipelime.sequences import Sample
from pipelime.stages import SampleStage


class StageAugmentations(SampleStage):
    def __init__(
        self,
        transform: t.Union[t.Dict[str, t.Any], str, A.BaseCompose, A.BasicTransform],
        keys_to_targets: t.Dict[str, str],
        output_key_format: t.Optional[str] = None,
    ):
        """Sample augmentation via Albumentations.

        :param transform: the albumentations transform or its serialized version as dict
            or JSON/YAML file
        :type transform: t.Union[t.Dict[str, t.Any],
                                 str, A.BaseCompose, A.BasicTransform]
        :param keys_to_targets: a mapping from key names to albumentation targets' names
        :type keys_to_targets: t.Dict[str, str]
        :param output_key_format: how to format the output keys. Any "*" will be
            replaced with the source key, eg, "aug_*_out" on ["image", "mask"] generates
            "aug_image_out" and "aug_mask_out". If no "*" is found, the string is
            suffixed to the source key, ie, "OutName" on "image" gives "imageOutName".
            If empty, the source key will be used.
        :type output_key_format: t.Optional[str]
        """
        super().__init__()

        if isinstance(transform, (A.BaseCompose, A.BasicTransform)):
            self._transform = transform
        elif isinstance(transform, str):
            self._transform = A.load(
                transform, data_format="json" if transform.endswith("json") else "yaml"
            )
        else:
            self._transform = A.from_dict(transform)

        # get an up-to-date description
        self._transform_cfg = A.to_dict(self._transform)

        self._keys_to_targets = keys_to_targets
        self._transform.add_targets(self._keys_to_targets)

        self._output_key_format = (
            "*"
            if output_key_format is None
            else (
                output_key_format
                if "*" in output_key_format
                else ("*" + output_key_format)
            )
        )

    def __call__(self, x: Sample) -> Sample:
        to_transform = {k: x[k]() for k in self._keys_to_targets if k in x}
        transformed = self._transform(**to_transform)  # type: ignore
        for k, v in transformed.items():
            x = x.set_value_as(self._output_key_format.replace("*", k), k, v)
        return x
