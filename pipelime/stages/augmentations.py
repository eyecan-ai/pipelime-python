from pathlib import Path
import albumentations as A
import typing as t
import pydantic as pyd

from pipelime.stages import SampleStage

if t.TYPE_CHECKING:
    from pipelime.sequences import Sample


class Transformation(pyd.BaseModel, extra="forbid", copy_on_model_validation="none"):
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

    transform: Transformation = pyd.Field(
        ...,
        description=(
            "The albumentations transformation defined as python object, "
            "serialized dict or yaml/json file."
        ),
    )
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
        transformed = self.transform.value(**to_transform)
        for k, v in transformed.items():
            x_key = self._target_to_keys[k]
            x = x.set_value_as(self.output_key_format.replace("*", x_key), x_key, v)
        return x
