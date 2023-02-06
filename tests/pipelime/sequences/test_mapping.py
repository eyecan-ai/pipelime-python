import pytest
import typing as t
import pipelime.sequences as pls
import pipelime.stages as plst


class TestMapping:
    def test_condition_probability(self):
        from pipelime.sequences.pipes.mapping import MappingConditionProbability

        cond = MappingConditionProbability(p=0.5, seed=42)  # type: ignore
        assert cond() is False
        assert cond() is True

    @pytest.mark.parametrize(
        ("start", "stop", "step"), [(0, None, 1), (-2, 3, -2), (3, 9, 3)]
    )
    def test_condition_index_range(self, start, stop, step):
        from pipelime.sequences.pipes.mapping import MappingConditionIndexRange

        seq = pls.SamplesSequence.from_callable(
            generator_fn=(lambda x: pls.Sample()), length=10
        )
        cond = MappingConditionIndexRange(start=start, stop=stop, step=step)

        if start < 0:
            start = 10 + start
        if stop is None:
            stop = 10
        elif stop < 0:
            stop = 10 + stop
        good_range = range(start, stop, step)

        for idx in range(10):
            assert cond(idx, seq[idx], seq) is (idx in good_range)

    def test_map(self, minimnist_dataset: dict):
        import pipelime.items as pli

        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        ).map(
            plst.StageLambda(
                lambda x: pls.Sample(
                    {k: pli.JsonMetadataItem({"the_answer": 42}) for k in x}
                )
            )
        )
        for sample in source:
            assert all(k in sample for k in minimnist_dataset["item_keys"])
            for k, v in sample.items():
                assert k in minimnist_dataset["item_keys"]
                assert isinstance(v, pli.JsonMetadataItem)

                raw = v()
                assert isinstance(raw, t.Mapping)
                assert "the_answer" in raw
                assert raw["the_answer"] == 42

    @pytest.mark.parametrize(
        ("start", "stop", "step"), [(0, None, 1), (-2, 3, -2), (3, 9, 3)]
    )
    def test_map_if(self, minimnist_dataset: dict, start, stop, step):
        import pipelime.items as pli
        from pipelime.sequences.pipes.mapping import MappingConditionIndexRange

        source = pls.SamplesSequence.from_underfolder(
            folder=minimnist_dataset["path"], merge_root_items=False
        ).map_if(
            stage=plst.StageLambda(
                lambda x: pls.Sample(
                    {k: pli.UnknownItem({"the_answer": 42}) for k in x}
                )
            ),
            condition=MappingConditionIndexRange(start=start, stop=stop, step=step),
        )

        if start < 0:
            start = len(source) + start
        if stop is None:
            stop = len(source)
        elif stop < 0:
            stop = len(source) + stop
        good_range = range(start, stop, step)

        for idx, sample in enumerate(source):
            assert all(k in sample for k in minimnist_dataset["item_keys"])
            for k, v in sample.items():
                assert k in minimnist_dataset["item_keys"]
                if idx in good_range:
                    assert isinstance(v, pli.UnknownItem)
                    raw = v()
                    assert isinstance(raw, t.Mapping)
                    assert "the_answer" in raw
                    assert raw["the_answer"] == 42
                else:
                    assert not isinstance(v, pli.UnknownItem)
