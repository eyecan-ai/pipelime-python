from pathlib import Path

import numpy as np
import pydantic
import pytest

import pipelime.utils.pydantic_types as plt

from ... import TestUtils


class TestNewPath:
    @pytest.mark.parametrize(
        ["path", "ext", "should_fail", "result"],
        [
            ("abc", None, False, "abc"),
            ("abc.", None, False, "abc."),
            ("abc.def", None, False, "abc.def"),
            (".abc", None, False, ".abc"),
            ("abc", "", False, "abc"),
            ("abc.", "", True, None),
            ("abc.def", "", True, None),
            (".abc", "", False, ".abc"),
            ("abc", ".", False, "abc."),
            ("abc.", ".", False, "abc."),
            ("abc.def", ".", True, None),
            (".abc", ".", False, ".abc."),
            ("abc", ".def", False, "abc.def"),
            ("abc.", ".def", True, None),
            ("abc.def", ".def", False, "abc.def"),
            (".abc", ".def", False, ".abc.def"),
            ("abc", "def", False, "abc.def"),
            ("abc.", "def", True, None),
            ("abc.def", "def", False, "abc.def"),
            (".abc", "def", False, ".abc.def"),
        ],
    )
    def test_create(self, path, ext, should_fail, result):
        class FooModel(pydantic.BaseModel):
            i1: plt.NewPath
            i2: plt.new_file_path(ext)  # type: ignore

        if should_fail:
            with pytest.raises(pydantic.ValidationError, match="i2"):
                _ = FooModel(i1=path, i2=path)
        else:
            foo = FooModel(i1=path, i2=path)
            assert str(foo.i1) == path
            assert str(foo.i2) == result

    def test_exists(self):
        with pytest.raises(ValueError):
            plt.NewPath.validate(Path(""))
        with pytest.raises(ValueError):
            plt.NewPath.validate(Path(__file__))


class TestNumpyType:
    def test_create(self):
        with pytest.raises(pydantic.ValidationError):
            _ = plt.NumpyType()  # type: ignore

        target = np.arange(12).reshape(2, 3, 2)
        src_list = [[[0, 1], [2, 3], [4, 5]], [[6, 7], [8, 9], [10, 11]]]

        nt = plt.NumpyType(__root__=np.array(src_list))
        assert TestUtils.numpy_eq(nt.value, target)

        nt = plt.NumpyType.create(src_list)  # type: ignore
        assert TestUtils.numpy_eq(nt.value, target)

        nt = plt.NumpyType.create(
            {
                "object": src_list,
                "dtype": "float16",
            }
        )
        assert TestUtils.numpy_eq(nt.value, target.astype("float16"))

        nt2 = plt.NumpyType.create(nt)
        assert TestUtils.numpy_eq(nt.value, nt2.value)

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
        assert TestUtils.numpy_eq(nt.value, nt_again.value)

        nt_again = pydantic.parse_obj_as(plt.NumpyType, nt.dict()["__root__"])
        assert nt.value.flags == nt_again.value.flags
        assert TestUtils.numpy_eq(nt.value, nt_again.value)


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

    def test_hash(self):
        from pipelime.items import MetadataItem

        itp = plt.ItemType(__root__=MetadataItem)
        assert hash(itp) == hash(MetadataItem)

        d = {itp: 42}  # type: ignore
        assert d[itp] == 42


def a_callable(a: int, b="c", **kwargs) -> str:
    return f"{a}{b}{kwargs}"


def b_callable(a: int, b="c", *args) -> str:
    return f"{a}{b}{args}"


class TestCallableDef:
    def test_create(self):
        import pipelime.choixe.utils.io

        cd = plt.CallableDef.create(a_callable)
        assert cd.value is a_callable

        cd = plt.CallableDef.create("pipelime.choixe.utils.io.load")
        assert cd.value is pipelime.choixe.utils.io.load

        cd2 = plt.CallableDef.create(cd)
        assert cd.value is cd2.value

        with pytest.raises(ValueError):
            _ = plt.CallableDef.create(12)  # type: ignore

    def test_signature(self):
        import inspect

        cd = plt.CallableDef(__root__=a_callable)

        assert cd.full_signature == inspect.signature(a_callable)
        assert cd.args_type == [int, None, None]
        assert not cd.has_var_positional
        assert cd.has_var_keyword
        assert cd.return_type is str

        cd = plt.CallableDef(__root__=b_callable)
        assert cd.full_signature == inspect.signature(b_callable)
        assert cd.args_type == [int, None, None]
        assert cd.has_var_positional
        assert not cd.has_var_keyword
        assert cd.return_type is str

    def test_call(self):
        cd = plt.CallableDef(__root__=a_callable)
        assert cd(42) == "42c{}"
        assert cd(49, "d", e=50) == "49d{'e': 50}"

    def test_serialize(self):
        cd = plt.CallableDef(__root__=a_callable)

        cd_again = pydantic.parse_raw_as(plt.CallableDef, cd.json())
        assert cd.value == cd_again.value

        cd_again = pydantic.parse_obj_as(plt.CallableDef, cd.dict()["__root__"])
        assert cd.value == cd_again.value

    def test_hash(self):
        cd = plt.CallableDef(__root__=a_callable)
        assert hash(cd) == hash(a_callable)

        d = {cd: 42}
        assert d[cd] == 42
