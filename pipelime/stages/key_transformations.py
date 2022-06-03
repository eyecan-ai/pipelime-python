import typing as t

import pydantic as pyd

from pipelime.sequences import Sample
from pipelime.stages import SampleStage


class StageDuplicateKey(SampleStage):
    source_key: str = pyd.Field(..., description="The key to duplicate.")
    copy_to: str = pyd.Field(
        ...,
        description=(
            "The new key name. If a key with this name already exists, "
            "it will NOT be overwritten."
        ),
    )

    def __call__(self, x: Sample) -> Sample:
        return x.duplicate_key(self.source_key, self.copy_to)


class StageKeyFormat(SampleStage):
    key_format: str = pyd.Field(
        "*",
        description=(
            "The new sample key format. Any `*` will be replaced with the "
            "source key, eg, `my_*_key` on [`image`, `mask`] generates "
            "`my_image_key` and `my_mask_key`. If no `*` is found, the string is "
            "suffixed to source key, ie, `MyKey` on `image` gives "
            "`imageMyKey`. If empty, the source key will not be changed."
        ),
    )
    apply_to: t.Sequence[str] = pyd.Field(
        default_factory=list,
        description=(
            "The keys to apply the new format to. Leave empty to apply to all keys."
        ),
    )

    @pyd.validator("key_format")
    def validate_key_format(cls, v):
        if "*" in v:
            return v
        return "*" + v

    def __call__(self, x: Sample) -> Sample:
        keys = list(x.keys())
        for k in keys:
            x = x.rename_key(k, self.key_format.replace("*", k))
        return x


class StageRemap(SampleStage):
    """Remaps keys in sample preserving internal values."""

    remap: t.Mapping[str, str] = pyd.Field(
        ..., description="`old_key: new_key` dictionary remapping."
    )
    remove_missing: bool = pyd.Field(
        True,
        description="If TRUE missing keys in remap will be removed "
        "in the output sample before name remapping",
    )

    def __call__(self, x: Sample) -> Sample:
        if self.remove_missing:
            x = x.extract_keys(*self.remap.keys())
        for kold, knew in self.remap.items():
            x = x.rename_key(kold, knew)
        return x


class StageKeysFilter(SampleStage):
    """Filter sample keys."""

    key_list: t.Sequence[str] = pyd.Field(..., description="List of keys to preserve.")
    negate: bool = pyd.Field(
        False,
        description=(
            "TRUE to delete `key_list`, FALSE delete all but keys in `key_list`."
        ),
    )

    def __call__(self, x: Sample) -> Sample:
        return (
            x.remove_keys(*self.key_list)
            if self.negate
            else x.extract_keys(*self.key_list)
        )
