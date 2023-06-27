import pytest
import pipelime.items as pli
import pipelime.sequences as pls
import pipelime.stages as plst


@pytest.fixture
def simple_sample():
    return pls.Sample(
        {
            "a": pli.YamlMetadataItem({"a": 10}),
            "b": pli.YamlMetadataItem({"b": 20}),
            "c": pli.YamlMetadataItem({"c": 30}, shared=True),
            "d": pli.YamlMetadataItem({"d": 40}, shared=True),
        },
    )


class TestStageShareItems:
    @pytest.mark.parametrize(
        "share, unshare",
        # list of disjoint sets.
        [
            ([], []),
            (["a"], []),
            ([], ["a"]),
            (["a"], ["b"]),
            (["a", "b"], []),
            ([], ["a", "b"]),
            (["a", "b"], ["d"]),
            (["a", "c"], ["b", "d"]),
            ([], ["a", "b", "c", "d"]),
            (["a", "b", "c", "d"], []),
        ],
    )
    def test_stage(self, simple_sample, share, unshare):
        expected = {k: v.is_shared for k, v in simple_sample.items()}
        for k in share:
            expected[k] = True
        for k in unshare:
            expected[k] = False

        stage = plst.StageShareItems(share=share, unshare=unshare)
        actual = stage(simple_sample)

        for k, v in actual.items():
            assert v.is_shared == expected[k]

    @pytest.mark.parametrize(
        "share, unshare",
        # list of non-disjoint sets.
        [
            (["a", "b"], ["b", "c"]),
            (["a", "b"], ["b", "a"]),
            (["a", "b"], ["a", "b"]),
            (["a", "b"], ["a", "b", "c"]),
        ],
    )
    def test_stage_fails_with_non_disjoint_sets(self, share, unshare):
        with pytest.raises(ValueError):
            plst.StageShareItems(share=share, unshare=unshare)
