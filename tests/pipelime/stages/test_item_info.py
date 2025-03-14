import pytest
from pydantic.v1 import parse_obj_as

from pipelime.sequences import Sample, SamplesSequence
from pipelime.stages import StageItemInfo
from pipelime.stages.item_info import ItemInfo


class TestItemInfo:
    def _str2item(self, item_name: str):
        from pipelime.choixe.utils.imports import import_symbol

        if "." not in item_name:
            item_name = "pipelime.items." + item_name
            return import_symbol(item_name)

    def test_item_info(self, minimnist_dataset: dict):
        source = SamplesSequence.from_underfolder(minimnist_dataset["path"])

        stage = StageItemInfo()
        source = source.map(stage)
        source.run()

        expected_info = {
            k: parse_obj_as(
                ItemInfo,
                {
                    "item_type": self._str2item(v),
                    "is_shared": (k in minimnist_dataset["root_keys"]),
                    "count": minimnist_dataset["len"],
                },
            )
            for k, v in minimnist_dataset["item_types"].items()
        }
        assert stage.items_info == expected_info

    def test_item_info_multiple_types(self):
        import pipelime.items as pli

        source = SamplesSequence.from_list(
            [
                Sample({"a": pli.YamlMetadataItem(42)}),
                Sample({"a": pli.JsonMetadataItem(42)}),
            ]
        )

        stage = StageItemInfo()
        source = source.map(stage)

        with pytest.raises(ValueError) as exc_info:
            source.run()
        assert "multiple types" in str(exc_info)

    def test_item_info_sharing_error(self):
        import pipelime.items as pli

        source = SamplesSequence.from_list(
            [
                Sample({"a": pli.YamlMetadataItem(42, shared=False)}),
                Sample({"a": pli.YamlMetadataItem(42, shared=True)}),
            ]
        )

        stage = StageItemInfo()
        source = source.map(stage)

        with pytest.raises(ValueError) as exc_info:
            source.run()
        assert "shared" in str(exc_info)
