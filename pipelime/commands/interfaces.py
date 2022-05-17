import typing as t
from pathlib import Path

import pydantic as pyd


class GrabberInterface(pyd.BaseModel, extra="forbid"):
    """Multiprocessing grabbing options."""

    num_workers: int = pyd.Field(
        0,
        description=(
            "The number of processes to spawn. If negative, "
            "the number of (logical) cpu cores is used."
        ),
    )
    prefetch: pyd.PositiveInt = pyd.Field(
        2, description="The number of samples loaded in advanced by each worker."
    )

    def grab_all(
        self,
        sequence,
        *,
        keep_order: bool,
        parent_cmd=None,
        track_message: str = "",
        sample_fn=None,
    ):
        from pipelime.sequences import Grabber, grab_all

        grabber = Grabber(
            num_workers=self.num_workers, prefetch=self.prefetch, keep_order=keep_order
        )
        size = len(sequence)
        track_fn = (
            None
            if parent_cmd is None
            else (lambda x: parent_cmd.track(x, size=size, message=track_message))
        )
        grab_all(grabber, sequence, track_fn=track_fn, sample_fn=sample_fn)


class SampleValidationInterface(pyd.BaseModel, extra="forbid"):
    """Sample schema validation."""

    sample_schema: t.Union[str, t.Mapping[str, str]] = pyd.Field(
        ...,
        description=(
            "The sample schema to validate, ie, a mapping from sample keys to expected "
            "item types.\nThe schema can be a class path to a pydantic model where "
            "fields' names are the sample keys, while fields' values are the item "
            "types. Otherwise, an explicit `key: item-classpath` mapping must be "
            "provided."
        ),
    )
    validators: t.Optional[t.Mapping[str, str]] = pyd.Field(
        None,
        description=(
            "When `sample_schema` is an explicit mapping, validators "
            "can be set for each field as a class path to a callable accepting the "
            "field value and either returning a validated version or raising an "
            "exception in case of error."
        ),
    )
    ignore_extra_keys: bool = pyd.Field(
        True,
        description=(
            "When `sample_schema` is an explicit mapping, if `ignore_extra_keys` is "
            "True, unexpected keys are ignored. Otherwise an error is raised."
        ),
    )
    lazy: bool = pyd.Field(
        True, description="If True, samples will be validated only when accessed."
    )
    max_samples: int = pyd.Field(
        -1,
        description=(
            "When the validation is NOT lazy, "
            "only the slice `[0:max_samples]` is checked."
        ),
    )

    _schema_model: t.Type[pyd.BaseModel] = pyd.PrivateAttr(pyd.BaseModel)

    def _make_pydantic_schema(self):
        from pipelime.choixe.utils.imports import import_symbol
        from pipelime.items import Item

        if isinstance(self.sample_schema, str):
            imported_v = import_symbol(self.sample_schema)
            if not issubclass(imported_v, pyd.BaseModel):
                raise ValueError(f"`{self.sample_schema}` is not a pydantic model.")
            self._schema_model = imported_v
        elif isinstance(self.sample_schema, t.Mapping):
            item_map = {
                k: import_symbol(cp if "." in cp else f"pipelime.items.{cp}")
                for k, cp in self.sample_schema.items()
            }
            for item_cls in item_map.values():
                if not issubclass(item_cls, Item):
                    raise ValueError(
                        "Model mapping must be a `key: item-class` mapping."
                    )

            class Config(pyd.BaseConfig):
                arbitrary_types_allowed = True
                extra = pyd.Extra.ignore if self.ignore_extra_keys else pyd.Extra.forbid

            _validators = None
            if self.validators is not None:
                _validators = {}
                for k, v_cp in self.validators.items():
                    v_call = import_symbol(v_cp)

                    def _validator_wrapper(cls, val):
                        return v_call(val)

                    _validators[f"validate_{k}"] = pyd.validator(k)(_validator_wrapper)

            self._schema_model = pyd.create_model(
                "SampleSchema",
                __config__=Config,
                __validators__=_validators,  # type: ignore
                **item_map,
            )

    def __init__(self, **data):
        super().__init__(**data)
        self._make_pydantic_schema()

    def append_validator(self, sequence):
        return sequence.validate_samples(
            sample_schema=self._schema_model,
            lazy=self.lazy,
            max_samples=self.max_samples,
        )


class InputDatasetInterface(pyd.BaseModel, extra="forbid"):
    """Input dataset options."""

    folder: pyd.DirectoryPath = pyd.Field(..., description="Dataset root folder.")
    merge_root_items: bool = pyd.Field(
        True,
        description=(
            "Adds root items as shared items "
            "to each sample (sample values take precedence)."
        ),
    )
    schema_: t.Optional[SampleValidationInterface] = pyd.Field(
        None, alias="schema", description="Sample schema validation."
    )

    def create_reader(self):
        from pipelime.sequences import SamplesSequence

        reader = SamplesSequence.from_underfolder(  # type: ignore
            folder=self.folder, merge_root_items=self.merge_root_items, must_exist=True
        )
        if self.schema_ is not None:
            reader = self.schema_.append_validator(reader)
        return reader

    def __str__(self) -> str:
        return str(self.folder)


class SerializationModeInterface(
    pyd.BaseModel, extra="forbid", underscore_attrs_are_private=True
):
    """Serialization modes for items and keys."""

    override: t.Mapping[str, t.Optional[t.Union[str, t.Sequence[str]]]] = pyd.Field(
        default_factory=dict,
        description=(
            "Serialization modes overridden for specific item types, "
            "eg, `{CREATE_NEW_FILE: [ImageItem, my.package.MyItem, "
            "my/module.py:OtherItem]}`. An empty value applies to all items."
        ),
    )
    disable: t.Mapping[str, t.Union[str, t.Sequence[str]]] = pyd.Field(
        default_factory=dict,
        description=(
            "Serialization modes disabled for specific item types, "
            "eg, `{ImageItem: HARD_LINK, my.package.MyItem: [SYM_LINK, DEEP_COPY]}`. "
            "The special key `_` applies to all items."
        ),
    )
    keys: t.Mapping[str, str] = pyd.Field(
        default_factory=dict,
        description=(
            "Serialization modes overridden for specific sample keys, "
            "eg, `{image: CREATE_NEW_FILE}`."
        ),
    )

    _overridden_modes_cms: t.List[t.ContextManager]
    _disabled_modes_cms: t.List[t.ContextManager]

    def _get_class_list(
        self, cls_paths: t.Optional[t.Union[str, t.Sequence[str]]]
    ) -> list:
        from pipelime.choixe.utils.imports import import_symbol

        if not cls_paths:
            return []
        if isinstance(cls_paths, str):
            cls_paths = [cls_paths]
        cls_paths = [c if "." in c else f"pipelime.items.{c}" for c in cls_paths]
        return [import_symbol(c) for c in cls_paths]

    def __init__(self, **data):
        import pipelime.items as pli

        super().__init__(**data)

        self._overridden_modes_cms = [
            pli.item_serialization_mode(m, *self._get_class_list(c))
            for m, c in self.override.items()
        ]

        self._disabled_modes_cms = [
            pli.item_disabled_serialization_modes(m, *self._get_class_list(c))
            for c, m in self.disable.items()
        ]

    def __enter__(self):
        for cm in self._overridden_modes_cms:
            cm.__enter__()
        for cm in self._disabled_modes_cms:
            cm.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        for cm in self._disabled_modes_cms:
            cm.__exit__(exc_type, exc_value, traceback)
        for cm in self._overridden_modes_cms:
            cm.__exit__(exc_type, exc_value, traceback)


class OutputDatasetInterface(pyd.BaseModel, extra="forbid"):
    """Output dataset options."""

    folder: Path = pyd.Field(..., description="Dataset root folder.")
    zfill: t.Optional[int] = pyd.Field(None, description="Custom index zero-filling.")
    exists_ok: bool = pyd.Field(
        False, description="If False raises an error when `folder` exists."
    )
    serialization: SerializationModeInterface = pyd.Field(
        default_factory=SerializationModeInterface,
        description="Serialization modes for items and keys.",
    )
    schema_: t.Optional[SampleValidationInterface] = pyd.Field(
        None, alias="schema", description="Sample schema validation."
    )

    def serialization_cm(self) -> t.ContextManager:
        return self.serialization

    def append_writer(self, sequence):
        writer = sequence.to_underfolder(
            folder=self.folder,
            zfill=self.zfill,
            exists_ok=self.exists_ok,
            key_serialization_mode=self.serialization.keys,
        )
        if self.schema_ is not None:
            writer = self.schema_.append_validator(writer)
        return writer

    def __str__(self) -> str:
        return str(self.folder)
