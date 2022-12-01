import numpy as np
import pytest

import pipelime.sequences as pls
from ... import TestUtils


class TestSample:
    def _np_sample(self):
        import pipelime.items as pli

        data = {k: pli.NpyNumpyItem(np.random.rand(3, 4)) for k in ("a", "b", "c")}
        return pls.Sample(data), data

    def _mixed_sample(self):
        import pipelime.items as pli

        data = {
            "c": pli.JsonMetadataItem({"foo": "bar"}),
            "d": pli.NpyNumpyItem(np.random.rand(3, 4)),
            "e": pli.TxtNumpyItem(np.random.rand(4)),
        }
        return pls.Sample(data), data

    def test_creation(self):
        sample, data = self._np_sample()

        assert len(sample) == len(data)
        assert all(k in sample for k in data)
        assert all(TestUtils.numpy_eq(sample[k](), v()) for k, v in data.items())
        assert sample.to_dict() == {k: v() for k, v in data.items()}

    def test_to_schema(self):
        from pydantic import BaseConfig, Extra, create_model
        from typing import Optional

        import pipelime.items as pli

        class SampleConfig(BaseConfig):
            arbitrary_types_allowed = True
            extra = Extra.forbid

        sample, _ = self._mixed_sample()
        sample_schema = create_model(
            "SampleSchema",
            __config__=SampleConfig,
            **{k: (v, ...) for k, v in sample.to_schema().items()},
        )

        try:
            sample_schema(**sample)
        except Exception:
            assert False

        sample_schema = create_model(
            "SampleSchema",
            __config__=SampleConfig,
            c=(pli.MetadataItem, ...),
            d=(pli.NumpyItem, ...),
            e=(pli.Item, ...),
        )

        try:
            sample_schema(**sample)
        except Exception:
            assert False

        sample_schema = create_model(
            "SampleSchema",
            __config__=SampleConfig,
            c=(pli.MetadataItem, ...),
            d=(pli.NumpyItem, ...),
            e=(pli.ImageItem, ...),
        )

        with pytest.raises(Exception):
            sample_schema(**sample)

        class SampleConfig2(BaseConfig):
            arbitrary_types_allowed = True

        sample_schema = create_model(
            "SampleSchema",
            __config__=SampleConfig2,
            c=(pli.MetadataItem, ...),
            d=(pli.NumpyItem, ...),
            e_other=(Optional[pli.Item], None),
        )

        try:
            sample_schema(**sample)
        except Exception:
            assert False

    def test_shallow_copy(self):
        sample, _ = self._np_sample()
        other_sample = sample.shallow_copy()
        assert sample.keys() == other_sample.keys()
        assert all(v is other_sample[k] for k, v in sample.items())

    def test_deep_copy(self):
        sample, _ = self._np_sample()
        other_sample = sample.deep_copy()
        assert sample.keys() == other_sample.keys()
        assert all(v is not other_sample[k] for k, v in sample.items())

    def test_set_item(self):
        import pipelime.items as pli

        sample, data = self._np_sample()
        changed_key = next(iter(data))
        changed_item = pli.JsonMetadataItem({"a": 1})
        other_sample = sample.set_item(changed_key, changed_item)
        assert sample.keys() == other_sample.keys()
        assert all(
            (v is other_sample[k]) if k != changed_key else (v is not other_sample[k])
            for k, v in sample.items()
        )
        assert other_sample[changed_key] is changed_item

    def test_set_value_as(self):
        import pipelime.items as pli

        sample, data = self._mixed_sample()
        ref_key = next(k for k, v in data.items() if isinstance(v, pli.NumpyItem))
        target_key = "new_key"
        new_value = np.random.rand(4, 4)

        other_sample = sample.set_value_as(target_key, ref_key, new_value)

        assert all(k in other_sample for k in sample)
        assert all(k in sample for k in other_sample if k != target_key)
        assert all(
            (v is sample[k])
            if k != target_key
            else isinstance(v, sample[ref_key].__class__)
            for k, v in other_sample.items()
        )
        assert TestUtils.numpy_eq(other_sample[target_key](), new_value)

    def test_set_value(self):
        import pipelime.items as pli

        sample, data = self._mixed_sample()
        target_key = next(k for k, v in data.items() if isinstance(v, pli.NumpyItem))
        new_value = np.random.rand(4, 4)

        other_sample = sample.set_value(target_key, new_value)

        assert sample.keys() == other_sample.keys()
        assert all(
            (v is sample[k])
            if k != target_key
            else isinstance(v, sample[target_key].__class__)
            for k, v in other_sample.items()
        )
        assert TestUtils.numpy_eq(other_sample[target_key](), new_value)

    def test_deep_set(self):
        import pipelime.items as pli

        data = {
            "fir.st": pli.JsonMetadataItem(
                {"data": ["a", "b", {"c": 42}], "meta": 3.14}
            ),
            r"sec\ond": pli.JsonMetadataItem(["a", "b", {"c": 36}]),
        }
        sample = pls.Sample(data)

        other_sample = sample.deep_set(r"fir\.st.data[2].c", 17)
        assert sample["fir.st"]()["data"][2]["c"] == 42  # type: ignore
        assert other_sample["fir.st"]()["data"][2]["c"] == 17  # type: ignore

        other_sample = sample.deep_set(r"sec\ond[2].c", 23)
        assert sample[r"sec\ond"]()[2]["c"] == 36  # type: ignore
        assert other_sample[r"sec\ond"]()[2]["c"] == 23  # type: ignore

        other_sample = sample.deep_set(r"fir\.st.data[2].new[1].p", [1, 2, 3])
        assert "new" not in sample["fir.st"]()["data"][2]  # type: ignore
        assert "new" in other_sample["fir.st"]()["data"][2]  # type: ignore
        assert len(other_sample["fir.st"]()["data"][2]["new"]) == 2  # type: ignore
        assert other_sample["fir.st"]()["data"][2]["new"][1]["p"] == [  # type: ignore
            1,
            2,
            3,
        ]
        assert other_sample["fir.st"]()["data"][2]["new"][0] is None  # type: ignore

        other_sample = sample.deep_set(r"fir\.st", [1, 2, 3])
        assert not isinstance(sample["fir.st"](), list)
        assert other_sample["fir.st"]() == [1, 2, 3]

    def test_deep_get(self):
        import pipelime.items as pli

        json_data = {"data": ["a", "b", {"c": 42}], "meta": 3.14}
        data = {
            "fir.st": pli.JsonMetadataItem(json_data),
            r"sec\ond": pli.JsonMetadataItem(["a", "b", {"c": 36}]),
        }
        sample = pls.Sample(data)

        assert sample.deep_get(r"fir\.st.data.2.c") == 42
        assert sample.deep_get(r"fir\.st") == json_data
        assert sample.deep_get(r"sec\ond[2].c") == 36
        assert sample.deep_get("not.there", "default") == "default"
        assert sample.deep_get("notthere", "default") == "default"

    def test_match(self):
        sample, data = self._mixed_sample()
        assert sample.match(f"`c.foo` == '{data['c']()['foo']}'")

    def test_change_key_invalid(self):
        sample, _ = self._np_sample()
        other_sample = sample.change_key("__", "--", False)
        assert other_sample is sample

    @pytest.mark.parametrize("delete_old_key", [True, False])
    def test_change_key(self, delete_old_key):
        sample, data = self._np_sample()
        original_key = next(iter(data))
        new_key = "new_key"
        other_sample = sample.change_key(
            original_key, new_key, delete_old_key=delete_old_key
        )

        assert all(k in sample for k in data)
        assert new_key not in sample
        assert all(k in other_sample for k in data if k != original_key)
        assert new_key in other_sample
        assert (
            (original_key not in other_sample)
            if delete_old_key
            else (original_key in other_sample)
        )

    def test_duplicate_key(self):
        sample, data = self._np_sample()
        original_key = next(iter(data))
        new_key = "new_key"
        other_sample = sample.duplicate_key(original_key, new_key)

        assert all(k in sample for k in data)
        assert new_key not in sample
        assert all(k in other_sample for k in data)
        assert new_key in other_sample

    def test_rename_key(self):
        sample, data = self._np_sample()
        original_key = next(iter(data))
        new_key = "new_key"
        other_sample = sample.rename_key(original_key, new_key)

        assert all(k in sample for k in data)
        assert new_key not in sample
        assert all(k in other_sample for k in data if k != original_key)
        assert new_key in other_sample
        assert original_key not in other_sample

    def _check_removed_keys(self, sample, data, other_sample, keys_to_remove):
        assert all(k in sample for k in data)
        assert all(
            (k in other_sample)
            if (k not in keys_to_remove)
            else (k not in other_sample)
            for k in data
        )

    def test_remove_keys(self):
        sample, data = self._np_sample()
        keys_to_remove = [list(data.keys())[0], list(data.keys())[-1]]
        other_sample = sample.remove_keys(*keys_to_remove)
        self._check_removed_keys(sample, data, other_sample, keys_to_remove)

    def test_sub(self):
        from pipelime.items import UnknownItem

        sample, data = self._np_sample()
        keys_to_remove = [list(data.keys())[0], list(data.keys())[-1]]
        sub_sample = pls.Sample({k: UnknownItem() for k in keys_to_remove})
        other_sample = sample - sub_sample
        self._check_removed_keys(sample, data, other_sample, keys_to_remove)

    def _check_extracted_keys(self, sample, data, other_sample, keys_to_extract):
        assert all(k in sample for k in data)
        assert all(
            (k in other_sample) if (k in keys_to_extract) else (k not in other_sample)
            for k in data
        )

    def test_extract_keys(self):
        sample, data = self._np_sample()
        keys_to_extract = [list(data.keys())[0], list(data.keys())[-1]]
        other_sample = sample.extract_keys(*keys_to_extract)
        self._check_extracted_keys(sample, data, other_sample, keys_to_extract)

    def test_and(self):
        from pipelime.items import UnknownItem

        sample, data = self._np_sample()
        keys_to_extract = [list(data.keys())[0], list(data.keys())[-1]]
        and_sample = pls.Sample({k: UnknownItem() for k in keys_to_extract})
        other_sample = sample & and_sample
        self._check_extracted_keys(sample, data, other_sample, keys_to_extract)

    def _merge_test(self, fn):
        sample_1, data_1 = self._np_sample()
        sample_2, data_2 = self._mixed_sample()

        other_sample = fn(sample_1, sample_2)

        only_1 = {k for k in data_1.keys() if k not in data_2.keys()}
        only_2 = {k for k in data_2.keys() if k not in data_1.keys()}
        and_1_2 = data_1.keys() & data_2.keys()
        sum_1_2 = data_1.keys() | data_2.keys()

        assert all(k in sample_1 for k in data_1)
        assert all(k not in sample_1 for k in only_2)
        assert all(k in sample_2 for k in data_2)
        assert all(k not in sample_2 for k in only_1)

        assert all(k in other_sample for k in sum_1_2)

        assert all(
            v is sample_2[k] if k in data_2 else v is sample_1[k]
            for k, v in other_sample.items()
        )
        assert all(other_sample[k] is not sample_1[k] for k in and_1_2)

    def test_merge(self):
        self._merge_test(lambda x1, x2: x1.merge(x2))

    def test_update(self):
        self._merge_test(lambda x1, x2: x1.update(x2))

    def test_zip(self):
        self._merge_test(lambda x1, x2: x1.zip(x2))

    def test_add(self):
        self._merge_test(lambda x1, x2: x1 + x2)

    def test_or(self):
        self._merge_test(lambda x1, x2: x1 | x2)

    def test_xor(self):
        from pipelime.items import UnknownItem

        sample, data = self._np_sample()
        xor_keys = [list(data.keys())[0], list(data.keys())[-1], "new_key"]
        xor_sample = pls.Sample({k: UnknownItem() for k in xor_keys})
        other_sample = sample ^ xor_sample

        assert all(k in sample for k in data)
        assert all(
            (k in other_sample) if (k not in xor_keys) else (k not in other_sample)
            for k in data
        )
        assert all(
            (k in other_sample) if (k not in data) else (k not in other_sample)
            for k in xor_keys
        )

    def test_direct_access(self):
        sample, data = self._np_sample()
        dsmpl = sample.direct_access()

        assert len(dsmpl) == len(sample) == len(data)
        assert all(k in sample for k in data)
        assert all(k in dsmpl for k in data)
        assert all(k in sample for k in dsmpl)
        assert all(TestUtils.numpy_eq(sample[k](), v) for k, v in dsmpl.items())
        assert sample.to_dict() == {k: v for k, v in dsmpl.items()}
