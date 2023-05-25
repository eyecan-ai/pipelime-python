import pytest
import typing as t
from pipelime.sequences import Sample
from pipelime.items import UnknownItem, YamlMetadataItem


class TestKeyStages:
    def _check_target_pred(self, target: Sample, pred: Sample):
        from ... import TestAssert

        TestAssert.samples_equal(target, pred)

    def test_remap_keep(self):
        from pipelime.stages import StageRemap

        stage = StageRemap(remap={"a": "b"}, remove_missing=False)
        self._check_target_pred(
            Sample({"b": UnknownItem(10), "c": UnknownItem(20)}),
            stage(Sample({"a": UnknownItem(10), "c": UnknownItem(20)})),
        )

    def test_remap_remove(self):
        from pipelime.stages import StageRemap

        stage = StageRemap(remap={"a": "b"}, remove_missing=True)
        self._check_target_pred(
            Sample({"b": UnknownItem(10)}),
            stage(Sample({"a": UnknownItem(10), "c": UnknownItem(20)})),
        )

    def test_remap_keep_dont_overwrite(self):
        from pipelime.stages import StageRemap

        stage = StageRemap(remap={"a": "b"}, remove_missing=False)
        self._check_target_pred(
            Sample({"a": UnknownItem(10), "b": UnknownItem(20)}),
            stage(Sample({"a": UnknownItem(10), "b": UnknownItem(20)})),
        )

    def test_remap_remove_and_overwrite(self):
        from pipelime.stages import StageRemap

        stage = StageRemap(remap={"a": "b"}, remove_missing=True)
        self._check_target_pred(
            Sample({"b": UnknownItem(10)}),
            stage(Sample({"a": UnknownItem(10), "b": UnknownItem(20)})),
        )

    @pytest.mark.parametrize("input_keys", [["a", "b"], "a"])
    @pytest.mark.parametrize("negate", [True, False])
    def test_filter(self, input_keys: t.Sequence[str], negate: bool):
        from pipelime.stages import StageKeysFilter

        sample = Sample(
            {"a": UnknownItem(10), "b": UnknownItem(20), "c": UnknownItem(30)}
        )
        stage = StageKeysFilter(key_list=input_keys, negate=negate)

        if isinstance(input_keys, str):
            input_keys = [input_keys]

        self._check_target_pred(
            Sample(
                {
                    k: v.make_new(v())
                    for k, v in sample.items()
                    if (k in input_keys) is (not negate)
                }
            ),
            stage(sample),
        )

    def test_replace(self):
        from pipelime.stages import StageReplaceItem
        from pipelime.items import NpyNumpyItem

        stage = StageReplaceItem(key_item_map={"a": NpyNumpyItem, "b": NpyNumpyItem})  # type: ignore
        self._check_target_pred(
            Sample({"a": NpyNumpyItem(10), "c": UnknownItem(20)}),
            stage(Sample({"a": UnknownItem(10), "c": UnknownItem(20)})),
        )

    def test_duplicate(self):
        from pipelime.stages import StageDuplicateKey
        from pipelime.items import NpyNumpyItem

        stage = StageDuplicateKey(source_key="a", copy_to="b")
        self._check_target_pred(
            Sample({"a": NpyNumpyItem(10), "b": NpyNumpyItem(10)}),
            stage(Sample({"a": NpyNumpyItem(10)})),
        )

        # existing key is not overwritten
        self._check_target_pred(
            Sample({"a": NpyNumpyItem(10), "b": UnknownItem(20)}),
            stage(Sample({"a": NpyNumpyItem(10), "b": UnknownItem(20)})),
        )

    @pytest.mark.parametrize(
        ("key_format", "apply_to", "target_sample"),
        [
            (
                "*",
                None,
                Sample(
                    {"a": UnknownItem(10), "b": UnknownItem(20), "c": UnknownItem(30)}
                ),
            ),
            (
                "new_*",
                None,
                Sample(
                    {
                        "new_a": UnknownItem(10),
                        "new_b": UnknownItem(20),
                        "new_c": UnknownItem(30),
                    }
                ),
            ),
            (
                "New",
                None,
                Sample(
                    {
                        "aNew": UnknownItem(10),
                        "bNew": UnknownItem(20),
                        "cNew": UnknownItem(30),
                    }
                ),
            ),
            (
                "par_*_tial",
                ["a", "c"],
                Sample(
                    {
                        "par_a_tial": UnknownItem(10),
                        "b": UnknownItem(20),
                        "par_c_tial": UnknownItem(30),
                    }
                ),
            ),
            (
                "par_*_tial",
                "a",
                Sample(
                    {
                        "par_a_tial": UnknownItem(10),
                        "b": UnknownItem(20),
                        "c": UnknownItem(30),
                    }
                ),
            ),
            (
                "Str",
                ["b", "c"],
                Sample(
                    {
                        "a": UnknownItem(10),
                        "bStr": UnknownItem(20),
                        "cStr": UnknownItem(30),
                    }
                ),
            ),
            (
                "Str",
                "b",
                Sample(
                    {
                        "a": UnknownItem(10),
                        "bStr": UnknownItem(20),
                        "c": UnknownItem(30),
                    }
                ),
            ),
        ],
    )
    def test_key_format(
        self,
        key_format: str,
        apply_to: t.Optional[t.Sequence[str]],
        target_sample: Sample,
    ):
        from pipelime.stages import StageKeyFormat

        stage = StageKeyFormat(key_format=key_format, apply_to=apply_to)
        self._check_target_pred(
            target_sample,
            stage(
                Sample(
                    {"a": UnknownItem(10), "b": UnknownItem(20), "c": UnknownItem(30)}
                )
            ),
        )

    @pytest.mark.parametrize(
        ("key_path", "value", "target_sample"),
        [
            (
                "a.b",
                "newv",
                Sample(
                    {
                        "a": YamlMetadataItem({"b": "newv", "c": ["fourty two"]}),
                        "d": UnknownItem(20),
                    }
                ),
            ),
            (
                "a.c[2]",
                [1, 2, 3],
                Sample(
                    {
                        "a": YamlMetadataItem(
                            {"b": 42, "c": ["fourty two", None, [1, 2, 3]]}
                        ),
                        "d": UnknownItem(20),
                    }
                ),
            ),
            (
                "a.e.f",
                {"g": True},
                Sample(
                    {
                        "a": YamlMetadataItem(
                            {"b": 42, "c": ["fourty two"], "e": {"f": {"g": True}}}
                        ),
                        "d": UnknownItem(20),
                    }
                ),
            ),
        ],
    )
    def test_set_meta(self, key_path, value, target_sample):
        from pipelime.stages import StageSetMetadata

        stage = StageSetMetadata(key_path=key_path, value=value)
        self._check_target_pred(
            target_sample,
            stage(
                Sample(
                    {
                        "a": YamlMetadataItem({"b": 42, "c": ["fourty two"]}),
                        "d": UnknownItem(20),
                    }
                )
            ),
        )
