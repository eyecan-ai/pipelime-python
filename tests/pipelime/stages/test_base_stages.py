import pytest

from pipelime.items import UnknownItem
from pipelime.sequences import Sample


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

        stage = StageLambda(func=(lambda x: x.set_item("a", UnknownItem(x["a"]() * 2))))
        self._check_target_pred(
            Sample({"a": UnknownItem(20)}), stage(Sample({"a": UnknownItem(10)}))
        )

    def test_compose(self):
        from pipelime.stages import StageCompose, StageLambda

        stage = StageCompose(
            [
                StageLambda(
                    func=(lambda x: x.set_item("a", UnknownItem(x["a"]() * 2)))
                ),
                StageLambda(
                    func=(lambda x: x.set_item("a", UnknownItem(x["a"]() + 5)))
                ),
            ]
        )
        self._check_target_pred(
            Sample({"a": UnknownItem(25)}), stage(Sample({"a": UnknownItem(10)}))
        )

    def test_shift(self):
        from pipelime.stages import StageCompose, StageLambda

        first_stage = StageLambda(
            func=(lambda x: x.set_item("a", UnknownItem(x["a"]() * 2)))
        )
        second_stage = StageLambda(
            func=(lambda x: x.set_item("a", UnknownItem(x["a"]() + 5)))
        )
        target = Sample({"a": UnknownItem(25)})

        self._check_target_pred(
            target, (first_stage >> second_stage)(Sample({"a": UnknownItem(10)}))
        )
        self._check_target_pred(
            target, (second_stage << first_stage)(Sample({"a": UnknownItem(10)}))
        )

    def test_stage_input(self):
        from pydantic.v1 import BaseModel

        from pipelime.stages import StageInput, StageKeyFormat

        def _check_stage(stage_input):
            assert isinstance(stage_input.__root__, StageKeyFormat)
            s_in = Sample({"a": UnknownItem(42), "b": UnknownItem(47)})
            s_out = stage_input(s_in)
            assert s_out.keys() == {"a", "b"}
            assert s_out["a"] is s_in["a"]
            assert s_out["b"] is s_in["b"]

        class Dummy(BaseModel):
            stg: StageInput

        # diret assignment
        ref_stage = StageKeyFormat(key_format="*")
        ref_stage_input = StageInput(__root__=ref_stage)
        _check_stage(ref_stage_input)

        # validation: another StageInput
        stage_input = Dummy.parse_obj({"stg": ref_stage_input})
        _check_stage(stage_input.stg)

        # validation: a Stage
        stage_input = Dummy.parse_obj({"stg": ref_stage})
        _check_stage(stage_input.stg)

        # validation: a Stage title
        stage_input = Dummy.parse_obj({"stg": "format-key"})
        _check_stage(stage_input.stg)

        # validation: a Stage dict
        stage_input = Dummy.parse_obj({"stg": {"format-key": {"key_format": "*"}}})
        _check_stage(stage_input.stg)

        with pytest.raises(ValueError):
            Dummy.parse_obj({"stg": 42})
            Dummy.parse_obj({"stg": "unknown-stage"})
