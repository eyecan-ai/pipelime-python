from pathlib import Path
import albumentations as A
import typing as t
import pydantic as pyd

from pipelime.sequences import Sample
from pipelime.stages import SampleStage


class StageAlbumentations(SampleStage):
    """Sample augmentation via Albumentations."""

    transform: t.Union[
        t.Mapping[str, t.Any], str, Path, A.BaseCompose, A.BasicTransform
    ] = pyd.Field(
        ...,
        description=(
            "The albumentations transform as well as its "
            "serialized version as dict or JSON/YAML file."
        ),
    )
    keys_to_targets: t.Mapping[str, str] = pyd.Field(
        ..., description="A mapping from key names to albumentation targets' names."
    )
    output_key_format: str = pyd.Field(
        "",
        description=(
            "How to format the output keys. Any `*` will be replaced with the "
            "source key, eg, `aug_*_out` on [`image`, `mask`] generates "
            "`aug_image_out` and `aug_mask_out`. If no `*` is found, the string is "
            "suffixed to the source key, ie, `OutName` on `image` gives "
            "`imageOutName`. If empty, the source key will be used as-is."
        ),
    )

    _trobj: t.Union[A.BaseCompose, A.BasicTransform]
    _target_to_keys: t.MutableMapping[str, str] = {}

    class Config:
        underscore_attrs_are_private = True
        arbitrary_types_allowed = True

    @pyd.validator("transform")
    def load_transform(cls, v):
        if isinstance(v, (str, Path)):
            v = str(v)
            return A.load(v, data_format="json" if v.endswith("json") else "yaml")
        elif isinstance(v, t.Mapping):
            return A.from_dict(v)
        else:
            return v

    @pyd.validator("output_key_format")
    def validate_output_key_format(cls, v):
        if "*" in v:
            return v
        return "*" + v

    def __init__(self, **data):
        super().__init__(**data)
        assert isinstance(self.transform, (A.BaseCompose, A.BasicTransform))

        self._trobj = self.transform
        self.transform = A.to_dict(self._trobj)

        target_counter: t.Mapping[str, int] = {}
        target_types: t.Mapping[str, str] = {}

        for key, trg in self.keys_to_targets.items():
            counter = target_counter.get(trg, 0)
            trg_name = trg if counter == 0 else f"{trg}_{counter}"
            target_counter[trg] = counter + 1
            target_types[trg_name] = trg
            self._target_to_keys[trg_name] = key

        self._trobj.add_targets(target_types)

    def __call__(self, x: Sample) -> Sample:
        to_transform = {k: x[v]() for k, v in self._target_to_keys.items() if v in x}
        transformed = self._trobj(**to_transform)  # type: ignore
        for k, v in transformed.items():
            x_key = self._target_to_keys[k]
            x = x.set_value_as(self.output_key_format.replace("*", x_key), x_key, v)
        return x
