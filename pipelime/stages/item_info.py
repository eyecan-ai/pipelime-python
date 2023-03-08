import typing as t

import pydantic as pyd

from pipelime.stages import SampleStage
from pipelime.items import Item

if t.TYPE_CHECKING:
    from pipelime.sequences import Sample


class ItemInfo(pyd.BaseModel):
    """Item infos estracted from samples."""

    item_type: t.Type[Item] = pyd.Field(..., description="The item type.")
    is_shared: bool = pyd.Field(..., description="Whether the item is shared.")
    count_: int = pyd.Field(
        1, alias="count", description="The number of samples owning this item."
    )


class StageItemInfo(SampleStage, title="item-info"):
    """Collects item infos from samples.
    WARNING: this stage CANNOT be combined with MULTIPROCESSING.
    """

    _items_info: t.MutableMapping[str, ItemInfo] = pyd.PrivateAttr(default_factory=dict)

    @property
    def items_info(self):
        return self._items_info

    def __call__(self, x: "Sample") -> "Sample":
        for k, v in x.items():
            if k in self._items_info:
                if self._items_info[k].item_type != v.__class__:
                    raise ValueError(
                        f"Key {k} has multiple types: "
                        f"{self._items_info[k].item_type} and {v.__class__}."
                    )
                if self._items_info[k].is_shared != v.is_shared:
                    raise ValueError(f"Key {k} is not always shared or not shared.")
                self._items_info[k].count_ += 1
            else:
                self._items_info[k] = ItemInfo(
                    item_type=v.__class__, is_shared=v.is_shared
                )  # type: ignore
        return x
