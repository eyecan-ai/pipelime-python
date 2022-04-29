import typing as t
import pydantic as pyd

from pipelime.sequences import Sample
from pipelime.stages import SampleStage


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
