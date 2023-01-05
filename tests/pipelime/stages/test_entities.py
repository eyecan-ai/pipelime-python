import pytest
from typing import Optional
from pathlib import Path
from pydantic import BaseModel, ValidationError, parse_obj_as
import numpy as np
from pipelime.stages import BaseEntity, EntityAction, StageEntity
from pipelime.sequences import Sample
from pipelime.stages.entities import ParsedItem, DynamicKey
import pipelime.items as pli


class MyInput0(BaseEntity):
    image: pli.ImageItem
    label: pli.NumpyItem


class MyInput1(BaseEntity, extra="ignore"):
    image: pli.ImageItem
    label: pli.NumpyItem


class MyInput2(BaseEntity, extra="forbid"):
    image: pli.ImageItem
    label: pli.NumpyItem


class MyOutput0(MyInput0):
    meta: pli.MetadataItem


class MyOutput1(BaseEntity):
    meta: pli.MetadataItem


class MyOutput2(MyInput0, extra="ignore"):
    meta: pli.MetadataItem


class MyOutput3(BaseEntity, extra="ignore"):
    meta: pli.MetadataItem


class MyOutput4(MyInput0, extra="forbid"):
    meta: pli.MetadataItem


class MyOutput5(BaseEntity, extra="forbid"):
    meta: pli.MetadataItem


class MyModel(BaseModel):
    john: str


class OptionalEntity(BaseEntity):
    label: Optional[pli.NumpyItem]
    meta: Optional[ParsedItem[pli.MetadataItem, MyModel]]


class FooBarModel(BaseModel):
    foo: str


class DynamicKeyEntity(BaseEntity):
    _img2 = DynamicKey(pli.ImageItem)
    _label2 = DynamicKey(pli.NumpyItem, [4, 5, 6])
    _other2 = DynamicKey(ParsedItem[pli.MetadataItem, FooBarModel])


def my_action0(x):
    return MyOutput0.merge(x, meta=pli.YamlMetadataItem({"john": "doe"}))


def my_action1(x):
    return MyOutput1.merge(x, meta=pli.YamlMetadataItem({"john": "doe"}))


def my_action2(x):
    return MyOutput2.merge(x, meta=pli.YamlMetadataItem({"john": "doe"}))


def my_action3(x):
    return MyOutput3.merge(x, meta=pli.YamlMetadataItem({"john": "doe"}))


def my_action4(x):
    return MyOutput4.merge(x, meta=pli.YamlMetadataItem({"john": "doe"}))


def my_action5(x):
    return MyOutput5.merge(x, meta=pli.YamlMetadataItem({"john": "doe"}))


def my_annotated_action(x: MyInput0):
    return MyOutput0.merge(x, meta=pli.YamlMetadataItem({"john": "doe"}))


def my_noparam_action():
    return None


def my_kwonly_action(*, x: MyInput0):
    return None


def my_parsed_action0(x: MyInput0):
    return OptionalEntity.merge(x, meta=pli.MetadataItem.make_new({"john": "doe"}))


def my_parsed_action1(x: MyInput0):
    return OptionalEntity.merge(x, meta=pli.MetadataItem.make_new({"jane": "doe"}))


def my_parsed_action2(x: MyInput0):
    return OptionalEntity.merge(x, label=None, meta=None)


def my_dynkey_action(x: DynamicKeyEntity):
    img2 = x._img2.validate("image")
    lbl2 = x._label2.validate("label")
    other2 = x._other2.validate("other")
    return MyInput0(image=img2, label=lbl2, other=other2)  # type: ignore


class TestEntities:
    def create_sample(self):
        return Sample(
            {
                "image": pli.PngImageItem(
                    np.random.rand(24).reshape(2, 4, 3).astype(np.uint8)
                ),
                "label": pli.TxtNumpyItem([1, 2, 3]),
                "meta": pli.BinaryItem(b"asdf"),
                "other": pli.JsonMetadataItem({"foo": "bar"}),
            }
        )

    def _check_samples(self, input, output, propagate, no_input):
        if not no_input or propagate:
            assert input["image"] is output["image"]
            assert input["label"] is output["label"]
        else:
            assert "image" not in output
            assert "label" not in output
        assert isinstance(output["meta"], pli.YamlMetadataItem)
        assert output["meta"]() == {"john": "doe"}
        if propagate:
            assert input["other"] is output["other"]
        else:
            assert "other" not in output

    def _make_stage_test(self, stage, extra, no_input, should_not_parse=False):
        in_sample = self.create_sample()
        try:
            out_sample = stage(in_sample)
        except ValidationError:
            assert extra == "forbid" or should_not_parse
        else:
            self._check_samples(in_sample, out_sample, extra == "allow", no_input)

    def _make_test(self, action_fn, input_cls, extra, no_input, should_not_parse=False):
        se = StageEntity(EntityAction(action=action_fn, input_type=input_cls))
        self._make_stage_test(se, extra, no_input, should_not_parse)

    @pytest.mark.parametrize("input_cls", [MyInput0, MyInput1, MyInput2])
    def test_inputs(self, input_cls):
        self._make_test(my_action0, input_cls, input_cls.__config__.extra, False)

    @pytest.mark.parametrize(
        ("action_fn", "extra", "no_input"),
        [
            (my_action0, "allow", False),
            (my_action1, "allow", True),
            (my_action2, "ignore", False),
            (my_action3, "ignore", True),
            (my_action4, "forbid", False),
            (my_action5, "forbid", True),
        ],
    )
    def test_outputs(self, action_fn, extra, no_input):
        self._make_test(action_fn, MyInput0, extra, no_input)

    @pytest.mark.parametrize(
        ("action_fn", "input_cls"),
        [
            (my_annotated_action, MyInput0),
            (f"{Path(__file__)}:my_annotated_action", None),
        ],
    )
    def test_annotated_action(self, action_fn, input_cls):
        se = StageEntity(
            EntityAction(action=action_fn)  # type: ignore
            if input_cls is None
            else EntityAction(action=action_fn, input_type=input_cls)
        )
        self._make_stage_test(se, "allow", False)

        se = parse_obj_as(
            StageEntity,
            action_fn
            if input_cls is None
            else {"action": action_fn, "input_type": input_cls},
        )
        self._make_stage_test(se, "allow", False)

    def test_invalid_action(self):
        with pytest.raises(ValidationError):
            StageEntity(EntityAction(action=my_noparam_action))  # type: ignore
        with pytest.raises(ValidationError):
            StageEntity(EntityAction(action=my_kwonly_action))  # type: ignore
        with pytest.raises(ValidationError):
            StageEntity(
                EntityAction(action=my_annotated_action, input_type=MyInput1)  # type: ignore
            )
        with pytest.raises(ValidationError):
            StageEntity(EntityAction(action=my_action0))  # type: ignore

    def test_parsed_action(self):
        self._make_test(my_parsed_action0, MyInput0, MyInput0.__config__.extra, False)

        self._make_test(
            my_parsed_action1, MyInput0, MyInput0.__config__.extra, False, True
        )

        se = StageEntity(EntityAction(action=my_parsed_action2, input_type=MyInput0))  # type: ignore
        in_sample = self.create_sample()
        out_sample = se(in_sample)
        assert in_sample["image"] is out_sample["image"]
        assert {"image", "other"} == set(out_sample.keys())

    def test_lambda(self):
        se = StageEntity(
            EntityAction(
                action=(
                    lambda x: MyOutput0.merge(  # type: ignore
                        x, meta=pli.YamlMetadataItem({"john": "doe"})
                    )
                ),
                input_type=MyInput0,  # type: ignore
            )
        )
        self._make_stage_test(se, "allow", False)

    def test_dynamic_keys(self):
        from ... import TestUtils

        se = StageEntity(EntityAction(action=my_dynkey_action))  # type: ignore
        in_sample = self.create_sample().remove_keys("label")
        out_sample = se(in_sample)
        assert in_sample["image"] is out_sample["image"]
        assert TestUtils.numpy_eq(out_sample["label"](), np.array([4, 5, 6]))
        assert in_sample["other"] is out_sample["other"]

        in_sample = in_sample.set_value("other", {"bar": "baz"})
        with pytest.raises(ValidationError):
            se(in_sample)
