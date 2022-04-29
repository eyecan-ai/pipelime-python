import typing as t
import pydantic as pyd

from pipelime.sequences import Sample
from pipelime.items import Item
from pipelime.stages import SampleStage


class StageReplaceItem(SampleStage):
    """Replaces items in sample preserving internal values."""

    key_item_map: t.Mapping[str, t.Type[Item]] = pyd.Field(
        ...,
        description=(
            "A mapping `key: item_cls` returning the new item class for the key."
        ),
    )

    def __call__(self, x: Sample) -> Sample:
        for key, item_cls in self.key_item_map.items():
            if key in x:
                old_item = x[key]
                x = x.set_item(
                    key, item_cls.make_new(old_item, shared=old_item.is_shared)
                )
        return x
