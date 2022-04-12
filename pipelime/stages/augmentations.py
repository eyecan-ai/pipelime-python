from pathlib import Path
import albumentations as A
import typing as t

from pipelime.sequences import Sample
from pipelime.stages import SampleStage


class StageAlbumentations(SampleStage):
    def __init__(
        self,
        transform: t.Union[t.Mapping[str, t.Any], str, Path, A.BaseCompose, A.BasicTransform],
        keys_to_targets: t.Mapping[str, str],
        output_key_format: t.Optional[str] = None,
    ):
        """Sample augmentation via Albumentations.

        :param transform: the albumentations transform or its serialized version as dict
            or JSON/YAML file
        :type transform: t.Union[t.Mapping[str, t.Any],
                                 str, Path, A.BaseCompose, A.BasicTransform]
        :param keys_to_targets: a mapping from key names to albumentation targets' names
        :type keys_to_targets: t.Mapping[str, str]
        :param output_key_format: how to format the output keys. Any "*" will be
            replaced with the source key, eg, "aug_*_out" on ["image", "mask"] generates
            "aug_image_out" and "aug_mask_out". If no "*" is found, the string is
            suffixed to the source key, ie, "OutName" on "image" gives "imageOutName".
            If empty, the source key will be used.
        :type output_key_format: t.Optional[str]
        """
        super().__init__()

        if isinstance(transform, (str, Path)):
            transform = str(transform)
            self._transform = A.load(
                transform, data_format="json" if transform.endswith("json") else "yaml"
            )
        elif isinstance(transform, t.Mapping):
            self._transform = A.from_dict(transform)
        else:
            self._transform = transform

        target_counter: t.Mapping[str, int] = {}
        target_types: t.Mapping[str, str] = {}
        self._target_to_keys: t.Mapping[str, str] = {}
        for key, trg in keys_to_targets.items():
            counter = target_counter.get(trg, 0)
            trg_name = trg if counter == 0 else f"{trg}_{counter}"
            target_counter[trg] = counter + 1
            target_types[trg_name] = trg
            self._target_to_keys[trg_name] = key

        self._transform.add_targets(target_types)

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
        to_transform = {k: x[v]() for k, v in self._target_to_keys.items() if v in x}
        transformed = self._transform(**to_transform)  # type: ignore
        for k, v in transformed.items():
            x_key = self._target_to_keys[k]
            x = x.set_value_as(self._output_key_format.replace("*", x_key), x_key, v)
        return x
