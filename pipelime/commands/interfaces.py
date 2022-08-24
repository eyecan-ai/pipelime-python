from __future__ import annotations
import typing as t
from pathlib import Path
from urllib.parse import ParseResult

import pydantic as pyd


class PydanticFieldMixinBase:
    # override in derived clasess
    _default_type_description: t.ClassVar[t.Optional[str]] = None
    _compact_form: t.ClassVar[t.Optional[str]] = None

    @classmethod
    def _description(cls, user_desc: t.Optional[str]) -> t.Optional[str]:
        desc_list = []
        if user_desc is None:
            user_desc = cls._default_type_description
            if user_desc is not None:
                desc_list.append(user_desc)
            elif cls._compact_form is None:
                return None
        if cls._compact_form is not None:
            desc_list.append(f"-----Compact form: `{cls._compact_form}`")
        return "\n".join(desc_list)


class PydanticFieldWithDefaultMixin(PydanticFieldMixinBase):
    @classmethod
    def pyd_field(cls, *, description: t.Optional[str] = None, **kwargs):
        return pyd.Field(
            default_factory=cls,  # type: ignore
            description=cls._description(description),  # type: ignore
            **kwargs,
        )


class PydanticFieldNoDefaultMixin(PydanticFieldMixinBase):
    @classmethod
    def pyd_field(
        cls, *, is_required: bool = True, description: t.Optional[str] = None, **kwargs
    ):
        return pyd.Field(
            ... if is_required else None,
            description=cls._description(description),  # type: ignore
            **kwargs,
        )


class GrabberInterface(PydanticFieldWithDefaultMixin, pyd.BaseModel, extra="forbid"):
    """Multiprocessing grabbing options."""

    _default_type_description: t.ClassVar[t.Optional[str]] = "Grabber options."
    _compact_form: t.ClassVar[t.Optional[str]] = "<num_workers>[,<prefetch>]"

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

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value):
        if isinstance(value, GrabberInterface):
            return value

        if isinstance(value, (str, bytes, int)):
            data = {}
            if isinstance(value, int):
                data["num_workers"] = value
            else:
                wrks, _, pf = str(value).partition(",")
                data["num_workers"] = int(wrks)
                if pf:
                    data["prefetch"] = int(pf)
            value = data

        if isinstance(value, t.Mapping):
            return GrabberInterface(**value)
        raise ValueError("Invalid grabber definition.")

    def grab_all(
        self,
        sequence,
        *,
        keep_order: bool = False,
        parent_cmd=None,
        track_message: str = "",
        sample_fn=None,
        size: t.Optional[int] = None,
    ):
        """Runs the grabber on a sequence.
        NB: `sample_fn` always runs on the main process and may take just the sample
        or the sample and its index.

        Args:
            sequence: the sequence to grab, usually a SamplesSequence.
            keep_order (bool, optional): if True, `sample_fn` will always receive the
                sample in the correct order. Defaults to False.
            parent_cmd (_type_, optional): the pipelime command running the grabber is
                needed to correctly setup the progress bar. Defaults to None.
            track_message (str, optional): a message shown next to the progress bar.
                Defaults to "".
            sample_fn (_type_, optional): an optional function to run on each grabbed
                element, usually a Sample. The signature may be (elem) or (elem, index).
                Defaults to None.
            size (t.Optional[int], optional): the size of the sequence. If not given,
                `len(sequence)` is evaluated. Defaults to None.
        """
        from pipelime.sequences import Grabber, grab_all

        grabber = Grabber(
            num_workers=self.num_workers, prefetch=self.prefetch, keep_order=keep_order
        )
        track_fn = (
            None
            if parent_cmd is None
            else (
                lambda x: parent_cmd.track(
                    x,
                    size=len(sequence) if size is None else size,
                    message=track_message,
                )
            )
        )
        grab_all(grabber, sequence, track_fn=track_fn, sample_fn=sample_fn, size=size)


class ItemValidationModel(pyd.BaseModel, extra="forbid"):
    """Item schema validation."""

    class_path: str = pyd.Field(
        ...,
        description=(
            "The item class path. The default package `pipelime.item` can be omitted"
        ),
    )
    is_optional: bool = pyd.Field(
        True, description="Whether the item is required or optional."
    )
    is_shared: bool = pyd.Field(False, description="Whether the item is shared or not.")
    validator_: t.Optional[str] = pyd.Field(
        None,
        description=(
            "A class path to a callable accepting the item value and either returning "
            "a validated value or raising an exception in case of error."
        ),
        alias="validator",
    )

    _item_cls = pyd.PrivateAttr()
    _validator_callable = pyd.PrivateAttr()

    def __init__(self, **data):
        from pipelime.choixe.utils.imports import import_symbol
        from pipelime.items import Item

        super().__init__(**data)

        self._item_cls = import_symbol(
            self.class_path
            if "." in self.class_path
            else f"pipelime.items.{self.class_path}"
        )
        if not issubclass(self._item_cls, Item):
            raise ValueError("Item classes must inherit from `pipelime.items.Item`.")

        def _identity(v):
            return v

        self._validator_callable = (
            import_symbol(self.validator_) if self.validator_ else _identity
        )

    def make_field(self, key_name: str):
        return (
            self._item_cls,
            pyd.Field(default_factory=self._item_cls, alias=key_name)
            if self.is_optional
            else pyd.Field(..., alias=key_name),
        )

    def make_validator_method(self, field_name: str):
        import uuid

        # we need random names and dynamic function creation
        # to avoid reusing the same function name for validators
        # (yes, pydantic is really pedantic...)
        rnd_name = uuid.uuid1().hex

        _validator_wrapper = (
            "def validate_{}_fn(cls, v):\n".format(rnd_name)
            + "    if v.is_shared != {}:\n".format(self.is_shared)
            + "        raise ValueError(\n"
            + "            'Item must{}be shared.'\n".format(
                " not " if not self.is_shared else " "
            )
            + "        )\n"
            + "    return user_validator_{}(v)\n".format(rnd_name)
        )

        local_scope = {
            **globals(),
            f"user_validator_{rnd_name}": self._validator_callable,
        }
        exec(_validator_wrapper, local_scope)
        fn_helper = local_scope[f"validate_{rnd_name}_fn"]
        return pyd.validator(field_name)(fn_helper)


class SampleValidationInterface(pyd.BaseModel, extra="forbid"):
    """Sample schema validation."""

    sample_schema: t.Union[str, t.Mapping[str, ItemValidationModel]] = pyd.Field(
        ...,
        description=(
            "The sample schema to validate, ie, a mapping from sample keys to expected "
            "item types.\nThe schema can be a class path to a pydantic model where "
            "fields' names are the sample keys, while fields' values are the item "
            "types. Otherwise, an explicit `key-name: ItemValidationModel` mapping "
            "must be provided."
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
        1,
        description=(
            "When the validation is NOT lazy, "
            "only the slice `[0:max_samples]` is checked. "
            "Set to 0 to check all the samples."
        ),
    )

    _schema_model: t.Type[pyd.BaseModel] = pyd.PrivateAttr()

    def _import_schema(self, schema_path: str):
        from pipelime.choixe.utils.imports import import_symbol

        imported_schema = import_symbol(schema_path)
        if not issubclass(imported_schema, pyd.BaseModel):
            raise ValueError(f"`{schema_path}` is not a pydantic model.")
        return imported_schema

    def _make_schema(self, schema_def: t.Mapping[str, ItemValidationModel]):
        class Config(pyd.BaseConfig):
            arbitrary_types_allowed = True
            extra = pyd.Extra.ignore if self.ignore_extra_keys else pyd.Extra.forbid

        def _safe_name(k):
            return f"{k}___"

        _item_map = {_safe_name(k): v.make_field(k) for k, v in schema_def.items()}
        _validators = {
            f"validate_{k}": v.make_validator_method(_safe_name(k))
            for k, v in schema_def.items()
        }

        return pyd.create_model(
            "SampleSchema",
            __config__=Config,
            __validators__=_validators,
            **_item_map,
        )

    def __init__(self, **data):
        super().__init__(**data)
        self._schema_model = (
            self._import_schema(self.sample_schema)
            if isinstance(self.sample_schema, str)
            else self._make_schema(self.sample_schema)
        )

    def append_validator(self, sequence):
        return sequence.validate_samples(
            sample_schema=self._schema_model,
            lazy=self.lazy,
            max_samples=self.max_samples,
        )

    def as_pipe(self):
        return {
            "validate_samples": {
                "sample_schema": self._schema_model,
                "lazy": self.lazy,
                "max_samples": self.max_samples,
            }
        }


class InputDatasetInterface(PydanticFieldNoDefaultMixin, pyd.BaseModel, extra="forbid"):
    """Input dataset options."""

    _default_type_description: t.ClassVar[t.Optional[str]] = "Input dataset."
    _compact_form: t.ClassVar[t.Optional[str]] = "<folder>[,<skip_empty>]"

    folder: Path = pyd.Field(..., description="Dataset root folder.")
    merge_root_items: bool = pyd.Field(
        True,
        description=(
            "Adds root items as shared items "
            "to each sample (sample values take precedence)."
        ),
    )
    skip_empty: bool = pyd.Field(False, description="Filter out empty samples.")
    schema_: t.Optional[SampleValidationInterface] = pyd.Field(
        None, alias="schema", description="Sample schema validation."
    )

    @pyd.validator("folder")
    def resolve_folder(cls, v: Path):
        # see https://bugs.python.org/issue38671
        return v.resolve().absolute()

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value):
        if isinstance(value, InputDatasetInterface):
            return value

        if isinstance(value, (str, bytes)):
            fld, _, sk_emp = str(value).partition(",")
            data: t.Mapping[str, t.Any] = {"folder": fld}
            if sk_emp:
                # NB: the value is left as-is when it is not True nor False
                # to raise an error on validation
                data["skip_empty"] = (  # type: ignore
                    True
                    if sk_emp.lower() == "true"
                    else (False if sk_emp.lower() == "false" else sk_emp)
                )
            value = data

        if isinstance(value, t.Mapping):
            return InputDatasetInterface(**value)
        raise ValueError("Invalid input dataset definition.")

    def create_reader(self):
        from pipelime.sequences import SamplesSequence

        reader = SamplesSequence.from_underfolder(  # type: ignore
            folder=self.folder, merge_root_items=self.merge_root_items, must_exist=True
        )
        if self.skip_empty:
            reader = reader.filter(lambda x: not all(i.is_shared for i in x.values()))
        if self.schema_ is not None:
            reader = self.schema_.append_validator(reader)
        return reader

    def __repr__(self) -> str:
        return self.__piper_repr__()

    def __piper_repr__(self) -> str:
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
            "my/module.py:OtherItem]}`. A Null value applies to all items."
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
        if "_" in cls_paths:
            return []
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


class OutputDatasetInterface(
    PydanticFieldNoDefaultMixin, pyd.BaseModel, extra="forbid"
):
    """Output dataset options."""

    _default_type_description: t.ClassVar[t.Optional[str]] = "Output dataset."
    _compact_form: t.ClassVar[
        t.Optional[str]
    ] = "<folder>[,<exists_ok>[,<force_new_files>]]"

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

    @pyd.validator("folder")
    def resolve_folder(cls, v: Path):
        # see https://bugs.python.org/issue38671
        return v.resolve().absolute()

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value):
        if isinstance(value, OutputDatasetInterface):
            return value

        if isinstance(value, (str, bytes)):
            data = {}
            raw_data = str(value).split(",")
            data["folder"] = raw_data[0]
            if len(raw_data) > 1:
                # NB: the value is left as-is when it is not True nor False
                # to raise an error on validation
                data["exists_ok"] = (
                    True
                    if raw_data[1].lower() == "true"
                    else (False if raw_data[1].lower() == "false" else raw_data[1])
                )
            if len(raw_data) > 2:
                if raw_data[2].lower() == "true":
                    data["serialization"] = SerializationModeInterface(
                        override={"DEEP_COPY": None}
                    )
            value = data

        if isinstance(value, t.Mapping):
            return OutputDatasetInterface(**value)
        raise ValueError("Invalid output dataset definition.")

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

    def as_pipe(self):
        return {
            "to_underfolder": {
                "folder": self.folder,
                "zfill": self.zfill,
                "exists_ok": self.exists_ok,
                "key_serialization_mode": self.serialization.keys,
            },
            **({} if self.schema_ is None else self.schema_.as_pipe()),
        }

    def __repr__(self) -> str:
        return self.__piper_repr__()

    def __piper_repr__(self) -> str:
        return str(self.folder)


class UrlDataModel(pyd.BaseModel, extra="forbid", underscore_attrs_are_private=True):
    """URL data model."""

    scheme: str = pyd.Field(..., description="The addressing scheme, eg, `s3`.")
    user: str = pyd.Field("", description="The user name.")
    password: str = pyd.Field("", description="The user password.")
    host: str = pyd.Field(..., description="The host name or ip address.")
    port: t.Optional[pyd.NonNegativeInt] = pyd.Field(
        None, description="The optional port number."
    )
    bucket: str = pyd.Field(..., description="The path to the remote data bucket.")
    args: t.Mapping[str, str] = pyd.Field(
        default_factory=dict, description="Optional remote-specific arguments."
    )

    def get_url(self):
        from pipelime.remotes import make_remote_url

        return make_remote_url(
            scheme=self.scheme,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            bucket=self.bucket,
            **self.args,
        )


class RemoteInterface(
    PydanticFieldNoDefaultMixin,
    pyd.BaseModel,
    extra="forbid",
    underscore_attrs_are_private=True,
):
    """Remote data lake options."""

    _default_type_description: t.ClassVar[
        t.Optional[str]
    ] = "Remote data lakes addresses."
    _compact_form: t.ClassVar[t.Optional[str]] = "<url>"

    url: t.Union[str, UrlDataModel] = pyd.Field(
        ...,
        description=(
            "The remote data lake URL. You can user the format "
            "`s3://user:password@host:port/bucket?kw1=arg1:kw2=arg2`."
        ),
    )

    _parsed_url: ParseResult

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value):
        if isinstance(value, RemoteInterface):
            return value
        if isinstance(value, (str, bytes)):
            return RemoteInterface(url=str(value))
        if isinstance(value, t.Mapping):
            return RemoteInterface(**value)
        raise ValueError("Invalid remote definition.")

    def __init__(self, **data):
        from urllib.parse import urlparse

        super().__init__(**data)
        self._parsed_url = (
            self.url.get_url()
            if isinstance(self.url, UrlDataModel)
            else urlparse(self.url)
        )

    def get_url(self):
        return self._parsed_url


class ToyDatasetInterface(pyd.BaseModel, extra="forbid"):
    """Toy dataset creation options."""

    length: int = pyd.Field(..., description="The number of samples to generate.")
    with_images: bool = pyd.Field(True, description="Whether to generate images.")
    with_masks: bool = pyd.Field(
        True, description="Whether to generate masks with object labels."
    )
    with_instances: bool = pyd.Field(
        True, description="Whether to generate images with object indexes."
    )
    with_objects: bool = pyd.Field(True, description="Whether to generate objects.")
    with_bboxes: bool = pyd.Field(
        True, description="Whether to generate objects' bboxes."
    )
    with_kpts: bool = pyd.Field(
        True, description="Whether to generate objects' keypoints."
    )
    image_size: int = pyd.Field(256, description="The size of the generated images.")
    key_format: str = pyd.Field(
        "*",
        description=(
            "The sample key format. Any `*` will be replaced with the "
            "base key name, eg, `my_*_key` on [`image`, `mask`] generates "
            "`my_image_key` and `my_mask_key`. If no `*` is found, the string is "
            "suffixed to the base key name, ie, `MyKey` on `image` gives "
            "`imageMyKey`. If empty, the base key name will be used."
        ),
    )
    max_labels: int = pyd.Field(
        5, description="The maximum number of object labels in the dataset."
    )
    objects_range: t.Tuple[int, int] = pyd.Field(
        (1, 5), description="The (min, max) number of objects in each sample."
    )

    @pyd.validator("key_format")
    def validate_key_format(cls, v):
        if "*" in v:
            return v
        return "*" + v

    def create_dataset_generator(self):
        from pipelime.sequences import SamplesSequence

        return SamplesSequence.toy_dataset(  # type: ignore
            length=self.length,
            with_images=self.with_images,
            with_masks=self.with_masks,
            with_instances=self.with_instances,
            with_objects=self.with_objects,
            with_bboxes=self.with_bboxes,
            with_kpts=self.with_kpts,
            image_size=self.image_size,
            key_format=self.key_format,
            max_labels=self.max_labels,
            objects_range=self.objects_range,
        )


yaml_any_type = t.Union[None, str, int, float, bool, t.Mapping[str, t.Any], t.Sequence]


class YamlInput(pyd.BaseModel, extra="forbid"):
    """General yaml/json data (str, number, mapping, list...) optionally loaded from
    a yaml/json file, possibly with key path (format <filepath>[:<key>])."""

    __root__: yaml_any_type

    @property
    def value(self):
        return self.__root__

    def __str__(self) -> str:
        return str(self.__root__)

    def __repr__(self) -> str:
        return repr(self.__root__)

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value):
        print(type(value))
        if isinstance(value, YamlInput):
            return value
        if isinstance(value, (str, bytes)):
            value = str(value)
            filepath, _, root_key = (
                value.rpartition(":") if ":" in value else (value, None, None)
            )
            filepath = Path(filepath)
            if filepath.exists():
                import yaml
                import pydash as py_

                with filepath.open() as f:
                    value = yaml.safe_load(f)
                    if root_key is not None:
                        value = py_.get(value, root_key, default=None)
            return YamlInput(__root__=value)  # type: ignore
        if cls._check_any_type(value):
            return YamlInput(__root__=value)
        raise ValueError(f"Invalid yaml data input: {value}")

    # @classmethod
    # def _check_mapping(cls, value):
    #     for k, v in value.items():
    #         if not isinstance(k, str):
    #             return False
    #         if not cls._check_any_type(v):
    #             return False
    #     return True
    #
    # @classmethod
    # def _check_sequence(cls, value):
    #     for v in value:
    #         if not cls._check_any_type(v):
    #             return False
    #     return True

    @classmethod
    def _check_any_type(cls, value):
        if isinstance(value, (str, int, float, bool, t.Sequence)) or value is None:
            return True
        if isinstance(value, t.Mapping):
            return all(isinstance(k, str) for k in value)
