from pathlib import Path
from typing import Optional

import pydantic.v1 as pyd
import pytest

import pipelime.items as pli
from pipelime.sequences import SamplesSequence, SampleValidationInterface


class TestValidation:
    class MyFullSchemaBase(pyd.BaseModel, arbitrary_types_allowed=True):
        cfg: pli.YamlMetadataItem
        numbers: pli.TxtNumpyItem
        pose: pli.TxtNumpyItem
        image: pli.PngImageItem
        label: pli.TxtNumpyItem
        mask: pli.PngImageItem
        metadata: pli.JsonMetadataItem
        values: pli.YamlMetadataItem
        points: pli.TxtNumpyItem
        invalid_key: Optional[pli.TxtNumpyItem] = None

        @pyd.validator("cfg")
        def validate_cfg(cls, v):
            if not v.is_shared:
                raise ValueError("cfg must be shared")
            return v

    class MyFullSchemaIgnoreExtra(MyFullSchemaBase, extra=pyd.Extra.ignore):
        pass

    class MyFullSchemaForbidExtra(MyFullSchemaBase, extra=pyd.Extra.forbid):
        pass

    class MyInvalidSchema:
        cfg: pli.MetadataItem

    def _schema_check_helper(self, sequence, is_lazy, should_fail):
        try:
            for _ in sequence:
                pass
            assert not should_fail
        except ValueError:
            assert should_fail and is_lazy

    def _schema_check(self, dataset, schema_def, should_fail):
        try:
            self._schema_check_helper(
                SamplesSequence.from_underfolder(dataset).validate_samples(
                    sample_schema=schema_def
                ),
                schema_def.lazy,
                should_fail,
            )
        except ValueError:
            assert should_fail and not schema_def.lazy

        try:
            self._schema_check_helper(
                schema_def.append_validator(SamplesSequence.from_underfolder(dataset)),
                schema_def.lazy,
                should_fail,
            )
        except ValueError:
            assert should_fail and not schema_def.lazy

        assert schema_def.as_pipe() == {
            "validate_samples": {
                "sample_schema": schema_def.dict(by_alias=True),
            }
        }

    @pytest.mark.parametrize("lazy", [True, False])
    @pytest.mark.parametrize("ignore_extra_keys", [True, False])
    def test_validate_full_schema(
        self, minimnist_dataset: dict, lazy, ignore_extra_keys
    ):
        self._schema_check(
            minimnist_dataset["path"],
            SampleValidationInterface(  # type: ignore
                sample_schema=(
                    TestValidation.MyFullSchemaIgnoreExtra
                    if ignore_extra_keys
                    else TestValidation.MyFullSchemaForbidExtra
                ),
                lazy=lazy,
            ),
            should_fail=False,
        )

        self._schema_check(
            minimnist_dataset["path"],
            SampleValidationInterface(  # type: ignore
                sample_schema=(
                    f"{Path(__file__).as_posix()}:TestValidation."
                    + (
                        "MyFullSchemaIgnoreExtra"
                        if ignore_extra_keys
                        else "MyFullSchemaForbidExtra"
                    )
                ),
                lazy=lazy,
            ),
            should_fail=False,
        )

    @pytest.mark.parametrize("lazy", [True, False])
    @pytest.mark.parametrize("ignore_extra_keys", [True, False])
    def test_validate_partial_schema(
        self, minimnist_dataset: dict, lazy, ignore_extra_keys
    ):
        from typing import Optional

        import pydantic.v1 as pyd

        import pipelime.items as pli

        class MySchema(
            pyd.BaseModel,
            arbitrary_types_allowed=True,
            extra="ignore" if ignore_extra_keys else "forbid",
        ):
            cfg: pli.YamlMetadataItem
            numbers: pli.TxtNumpyItem
            pose: pli.TxtNumpyItem
            invalid_key: Optional[pli.TxtNumpyItem] = None

        self._schema_check(
            minimnist_dataset["path"],
            SampleValidationInterface(sample_schema=MySchema, lazy=lazy),  # type: ignore
            should_fail=not ignore_extra_keys,
        )

    @pytest.mark.parametrize("lazy", [True, False])
    @pytest.mark.parametrize("ignore_extra_keys", [True, False])
    def test_validate_schema_with_base_classes(
        self, minimnist_dataset: dict, lazy, ignore_extra_keys
    ):
        from typing import Optional

        import pydantic.v1 as pyd

        import pipelime.items as pli

        class MySchema(
            pyd.BaseModel,
            arbitrary_types_allowed=True,
            extra=pyd.Extra.ignore if ignore_extra_keys else pyd.Extra.forbid,
        ):
            cfg: pli.MetadataItem
            numbers: pli.NumpyItem
            pose: pli.TxtNumpyItem
            image: pli.NumpyItem
            label: pli.Item
            mask: pli.ImageItem
            invalid_key: Optional[pli.Item] = None

        self._schema_check(
            minimnist_dataset["path"],
            SampleValidationInterface(sample_schema=MySchema, lazy=lazy),  # type: ignore
            should_fail=not ignore_extra_keys,
        )

    @pytest.mark.parametrize("lazy", [True, False])
    @pytest.mark.parametrize("ignore_extra_keys", [True, False])
    def test_validate_schema_from_dict(
        self, minimnist_dataset: dict, lazy: bool, ignore_extra_keys: bool
    ):
        schema_dict = {
            "cfg": {
                "class_path": "YamlMetadataItem",
                "is_optional": False,
                "is_shared": True,
            },
            "numbers": {
                "class_path": "TxtNumpyItem",
                "is_optional": False,
                "is_shared": True,
            },
            "pose": {
                "class_path": "TxtNumpyItem",
                "is_optional": False,
                "is_shared": True,
            },
            "image": {
                "class_path": "PngImageItem",
                "is_optional": False,
                "is_shared": False,
            },
            "label": {
                "class_path": "TxtNumpyItem",
                "is_optional": False,
                "is_shared": False,
            },
            "mask": {
                "class_path": "PngImageItem",
                "is_optional": False,
                "is_shared": False,
            },
        }

        self._schema_check(
            minimnist_dataset["path"],
            SampleValidationInterface(  # type: ignore
                sample_schema=schema_dict,
                ignore_extra_keys=ignore_extra_keys,
                lazy=lazy,
            ),
            should_fail=not ignore_extra_keys,
        )

    def test_invalid_schema_class(self):
        with pytest.raises(ValueError):
            svi = SampleValidationInterface(
                sample_schema=(  # type: ignore
                    f"{Path(__file__).as_posix()}:TestValidation.MyInvalidSchema"
                )
            )
            _ = svi.schema_model
