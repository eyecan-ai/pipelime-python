import typing as t

from pipelime.sequences import Sample
from pipelime.items import Item
from pipelime.stages import SampleStage


class StageReplaceItem(SampleStage):
    def __init__(self, key_item_map: t.Dict[str, t.Type[Item]]):
        self._key_item_map = key_item_map

    def __call__(self, x: Sample) -> Sample:
        for key, item_cls in self._key_item_map.items():
            if key in x:
                old_item = x[key]
                x = x.set_item(
                    key, item_cls.make_new(old_item, shared=old_item.is_shared)
                )
        return x
