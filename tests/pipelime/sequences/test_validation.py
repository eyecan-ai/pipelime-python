import pytest

from pipelime.sequences import SamplesSequence


class TestValidation:
    def _schema_check(self, dataset, schema_def, lazy, should_fail):
        try:
            seq = SamplesSequence.from_underfolder(  # type: ignore
                dataset
            ).validate_samples(sample_schema=schema_def, lazy=lazy)
            try:
                for _ in seq:
                    pass
                assert not should_fail
            except ValueError:
                assert should_fail and lazy
        except ValueError:
            assert should_fail and not lazy

    @pytest.mark.parametrize("lazy", [True, False])
    @pytest.mark.parametrize("ignore_extra_keys", [True, False])
    def test_validate_full_schema(
        self, minimnist_dataset: dict, lazy, ignore_extra_keys
    ):
        from typing import Optional

        import pydantic as pyd

        import pipelime.items as pli

        class MySchema(
            pyd.BaseModel,
            arbitrary_types_allowed=True,
            extra=pyd.Extra.ignore if ignore_extra_keys else pyd.Extra.forbid,
        ):
            cfg: pli.YamlMetadataItem
            numbers: pli.TxtNumpyItem
            pose: pli.TxtNumpyItem
            image: pli.PngImageItem
            label: pli.TxtNumpyItem
            mask: pli.PngImageItem
            metadata: pli.JsonMetadataItem
            points: pli.TxtNumpyItem
            invalid_key: Optional[pli.TxtNumpyItem] = None

        self._schema_check(
            minimnist_dataset["path"], MySchema, lazy=lazy, should_fail=False
        )

    @pytest.mark.parametrize("lazy", [True, False])
    @pytest.mark.parametrize("ignore_extra_keys", [True, False])
    def test_validate_partial_schema(
        self, minimnist_dataset: dict, lazy, ignore_extra_keys
    ):
        from typing import Optional

        import pydantic as pyd

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
            MySchema,
            lazy=lazy,
            should_fail=not ignore_extra_keys,
        )

    @pytest.mark.parametrize("lazy", [True, False])
    @pytest.mark.parametrize("ignore_extra_keys", [True, False])
    def test_validate_schema_with_base_classes(
        self, minimnist_dataset: dict, lazy, ignore_extra_keys
    ):
        from typing import Optional

        import pydantic as pyd

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
            MySchema,
            lazy=lazy,
            should_fail=not ignore_extra_keys,
        )
