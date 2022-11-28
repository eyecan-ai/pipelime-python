import pytest
import typing as t
from pipelime.sequences import Sample
from pipelime.items import UnknownItem


class TestKeyStages:
    def _check_target_pred(self, target: Sample, pred: Sample):
        assert all(
            k in target and type(target[k]) == type(v) and target[k]() == v()
            for k, v in pred.items()
        )
        assert all(
            k in pred and type(pred[k]) == type(v) and pred[k]() == v()
            for k, v in target.items()
        )

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

    def test_filter_positive(self):
        from pipelime.stages import StageKeysFilter

        stage = StageKeysFilter(key_list=["a", "b"], negate=False)
        self._check_target_pred(
            Sample({"a": UnknownItem(10), "b": UnknownItem(20)}),
            stage(
                Sample(
                    {"a": UnknownItem(10), "b": UnknownItem(20), "c": UnknownItem(30)}
                )
            ),
        )

    def test_filter_negate(self):
        from pipelime.stages import StageKeysFilter

        stage = StageKeysFilter(key_list=["a", "b"], negate=True)
        self._check_target_pred(
            Sample({"c": UnknownItem(30)}),
            stage(
                Sample(
                    {"a": UnknownItem(10), "b": UnknownItem(20), "c": UnknownItem(30)}
                )
            ),
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
                    {"a": UnknownItem(10), "b": UnknownItem(20), "c": UnknownItem(20)}
                ),
            ),
            (
                "new_*",
                None,
                Sample(
                    {
                        "new_a": UnknownItem(10),
                        "new_b": UnknownItem(20),
                        "new_c": UnknownItem(20),
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
                        "cNew": UnknownItem(20),
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
                        "par_c_tial": UnknownItem(20),
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
                        "cStr": UnknownItem(20),
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
                    {"a": UnknownItem(10), "b": UnknownItem(20), "c": UnknownItem(20)}
                )
            ),
        )
