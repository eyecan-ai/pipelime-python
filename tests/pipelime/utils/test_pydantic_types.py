import numpy as np
import pytest
import pydantic
import pipelime.utils.pydantic_types as plt


class TestNumpyType:
    def _np_eq(self, a, b):
        return np.array_equal(a, b, equal_nan=True)

    def test_create(self):
        with pytest.raises(pydantic.ValidationError):
            _ = plt.NumpyType()  # type: ignore

        target = np.arange(12).reshape(2, 3, 2)
        src_list = [[[0, 1], [2, 3], [4, 5]], [[6, 7], [8, 9], [10, 11]]]

        nt = plt.NumpyType(__root__=np.array(src_list))
        assert self._np_eq(nt.value, target)

        nt = plt.NumpyType.create(src_list)  # type: ignore
        assert self._np_eq(nt.value, target)

        nt = plt.NumpyType.create(
            {
                "object": src_list,
                "dtype": "float16",
            }
        )
        assert self._np_eq(nt.value, target.astype("float16"))

        nt2 = plt.NumpyType.create(nt)
        assert self._np_eq(nt.value, nt2.value)

        with pytest.raises(ValueError):
            _ = plt.NumpyType.create(
                {
                    "object": [1.2, "abc"],
                    "dtype": "float",
                }
            )

    def test_serialize(self):
        data = np.arange(12).reshape(2, 3, 2, order="F") * 3.1415 + 1.4142
        nt = plt.NumpyType(__root__=data.astype(np.float16))

        nt_again = pydantic.parse_raw_as(plt.NumpyType, nt.json())
        assert nt.value.flags == nt_again.value.flags
        assert self._np_eq(nt.value, nt_again.value)

        nt_again = pydantic.parse_obj_as(plt.NumpyType, nt.dict()["__root__"])
        assert nt.value.flags == nt_again.value.flags
        assert self._np_eq(nt.value, nt_again.value)


class TestYamlInput:
    @pytest.mark.parametrize(
        "value",
        [
            None,
            "abc",
            123,
            1.23,
            True,
            [1, False],
            {"a": 1, "b": "c"},
            [{"a": [1, 2, 3], "b": "c"}, {"a": {"b": 42}}],
        ],
    )
    def test_create(self, value):
        yi = plt.YamlInput.create(value)
        assert value == yi.value

        yi2 = plt.YamlInput.create(yi)
        assert yi2.value == yi.value

    def test_create_from_file(self, choixe_plain_cfg):
        import yaml

        with open(str(choixe_plain_cfg), "r") as f:
            value = yaml.safe_load(f)

        yi = plt.YamlInput.create(str(choixe_plain_cfg))
        assert value == yi.value

        yi = plt.YamlInput.create(str(choixe_plain_cfg) + ":charlie")
        assert value["charlie"] == yi.value

        with pytest.raises(ValueError):
            _ = plt.YamlInput.create(lambda: None)  # type: ignore

    def test_serialize(self):
        data = {"a": 1, "b": "c"}
        yi = plt.YamlInput(__root__=data)

        yi_again = pydantic.parse_raw_as(plt.YamlInput, yi.json())
        assert yi.value == yi_again.value

        yi_again = pydantic.parse_obj_as(plt.YamlInput, yi.dict()["__root__"])
        assert yi.value == yi_again.value


class TestItemType:
    def test_create(self):
        from pipelime.items import MetadataItem

        itp = plt.ItemType.create(MetadataItem)
        assert itp.value is MetadataItem

        itp = plt.ItemType.create("pipelime.items.MetadataItem")
        assert itp.value is MetadataItem

        itp = plt.ItemType.create("MetadataItem")
        assert itp.value is MetadataItem

        itp2 = plt.ItemType.create(itp)
        assert itp.value is itp2.value

        with pytest.raises(ValueError):
            _ = plt.ItemType.create(12)  # type: ignore

    def test_call(self):
        from pipelime.items import JsonMetadataItem

        itp = plt.ItemType(__root__=JsonMetadataItem)
        assert isinstance(itp(42), JsonMetadataItem)

    def test_serialize(self):
        from pipelime.items import MetadataItem

        itp = plt.ItemType(__root__=MetadataItem)

        itp_again = pydantic.parse_raw_as(plt.ItemType, itp.json())
        assert itp.value == itp_again.value

        itp_again = pydantic.parse_obj_as(plt.ItemType, itp.dict()["__root__"])
        assert itp.value == itp_again.value
