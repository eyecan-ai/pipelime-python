from pipelime.sequences import Sample
from pipelime.items import UnknownItem


class TestBaseStages:
    def _check_target_pred(self, target: Sample, pred: Sample):
        assert all(
            k in target and type(target[k]) == type(v) and target[k]() == v()
            for k, v in pred.items()
        )
        assert all(
            k in pred and type(pred[k]) == type(v) and pred[k]() == v()
            for k, v in target.items()
        )

    def test_identity(self):
        from pipelime.stages import StageIdentity

        stage = StageIdentity()
        sample = Sample({"a": UnknownItem()})
        assert stage(sample) is sample

    def test_lambda(self):
        from pipelime.stages import StageLambda

        stage = StageLambda(
            func=(lambda x: x.set_item("a", UnknownItem(x["a"]() * 2)))  # type: ignore
        )
        self._check_target_pred(
            Sample({"a": UnknownItem(20)}), stage(Sample({"a": UnknownItem(10)}))
        )

    def test_compose(self):
        from pipelime.stages import StageCompose, StageLambda

        stage = StageCompose(
            [
                StageLambda(
                    func=(
                        lambda x: x.set_item("a", UnknownItem(x["a"]() * 2))  # type: ignore
                    )
                ),
                StageLambda(
                    func=(
                        lambda x: x.set_item("a", UnknownItem(x["a"]() + 5))  # type: ignore
                    )
                ),
            ]
        )
        self._check_target_pred(
            Sample({"a": UnknownItem(25)}), stage(Sample({"a": UnknownItem(10)}))
        )
