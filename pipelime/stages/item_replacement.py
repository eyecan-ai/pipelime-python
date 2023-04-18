import typing as t
import pydantic as pyd

from pipelime.stages import SampleStage
from pipelime.utils.pydantic_types import ItemType, YamlInput

if t.TYPE_CHECKING:
    from pipelime.sequences import Sample


class StageReplaceItem(SampleStage, title="replace-item"):
    """Replaces items in sample preserving internal values."""

    key_item_map: t.Mapping[str, ItemType] = pyd.Field(
        ...,
        description=(
            "A mapping `key: item_cls` returning the new item class for the key."
        ),
    )

    def __call__(self, x: "Sample") -> "Sample":
        for key, item_cls in self.key_item_map.items():
            if key in x:
                old_item = x[key]
                x = x.set_item(
                    key, item_cls.value.make_new(old_item, shared=old_item.is_shared)
                )
        return x


class StageSetMetadata(SampleStage, title="set-meta"):
    """Sets metadata in samples."""

    key_path: str = pyd.Field(
        ..., description="The metadata key in pydash dot notation."
    )
    value: YamlInput = pyd.Field(
        None, description="The value to set, ie, any valid yaml/json value."
    )

    def __call__(self, x: "Sample") -> "Sample":
        return x.deep_set(self.key_path, self.value.value)
