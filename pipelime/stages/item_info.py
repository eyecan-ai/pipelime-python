import typing as t

import pydantic as pyd

from pipelime.stages import SampleStage


class ItemInfo(pyd.BaseModel):
    """Item infos estracted from samples."""

    class_path: str = pyd.Field(..., description="The item class path.")
    is_shared: bool = pyd.Field(..., description="Whether the item is shared.")
    count_: int = pyd.Field(
        1, alias="count", description="The number of samples owning this item."
    )


class StageItemInfo(SampleStage):
    """Collects item infos from samples.
    WARNING: this stage CANNOT be combined with MULTIPROCESSING.
    """

    _items_info: t.MutableMapping[str, ItemInfo] = pyd.PrivateAttr(default_factory=dict)

    def items_info(self):
        return self._items_info

    def __call__(self, x: "Sample") -> "Sample":
        for k, v in x.items():
            class_name = (
                v.__class__.__name__
                if v.__class__.__module__.startswith("pipelime.items")
                else f"{v.__class__.__module__}.{v.__class__.__name__}"
            )
            if k in self._items_info:
                if self._items_info[k].class_path != class_name:
                    raise ValueError(
                        f"Key {k} has multiple types: "
                        f"{self._items_info[k].class_path} and {class_name}."
                    )
                if self._items_info[k].is_shared != v.is_shared:
                    raise ValueError(f"Key {k} is not always shared or not shared.")
                self._items_info[k].count_ += 1
            else:
                self._items_info[k] = ItemInfo(
                    class_path=class_name, is_shared=v.is_shared
                )  # type: ignore
        return x
