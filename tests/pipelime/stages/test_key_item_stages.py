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

        stage = StageReplaceItem(key_item_map={"a": NpyNumpyItem})
        self._check_target_pred(
            Sample({"a": NpyNumpyItem(10), "c": UnknownItem(20)}),
            stage(Sample({"a": UnknownItem(10), "c": UnknownItem(20)})),
        )
