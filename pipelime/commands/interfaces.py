from __future__ import annotations
import typing as t
from pathlib import Path
from urllib.parse import ParseResult

import pydantic as pyd

from pipelime.utils.pydantic_types import SampleValidationInterface, ItemType


class PydanticFieldMixinBase:
    # override in derived clasess
    _default_type_description: t.ClassVar[t.Optional[str]] = None
    _compact_form: t.ClassVar[t.Optional[str]] = None

    @classmethod
    def _description(cls, user_desc: t.Optional[str]) -> t.Optional[str]:
        from pipelime.cli.pretty_print import _short_line

        desc_list = []
        if user_desc is None:
            user_desc = cls._default_type_description
        if user_desc is not None:  # pragma: no branch
            desc_list.append(user_desc)
        elif cls._compact_form is None:  # pragma: no cover
            return None
        if cls._compact_form is not None:  # pragma: no branch
            desc_list.append(f"{_short_line()} Compact form: `{cls._compact_form}`")
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
    """Multiprocessing grabbing options.

    Examples:
        How to use it in your command::

            class EasyCommand(PipelimeCommand, title="easy"):
                input: InputDatasetInterface = InputDatasetInterface.pyd_field(
                    alias="i", piper_port=PiperPortType.INPUT
                )
                output: OutputDatasetInterface = OutputDatasetInterface.pyd_field(
                        alias="o", piper_port=PiperPortType.OUTPUT
                )
                grabber: GrabberInterface = GrabberInterface.pyd_field(
                    alias="g"
                )

                def run(self):
                    seq = self.input.create_reader()
                    seq = self.output.append_writer(seq)
                    self.grabber.grab_all(
                        seq,
                        grab_context_manager=self.output.serialization_cm(),
                        keep_order=False,
                        parent_cmd=self,
                        track_message=f"Executing ({len(seq)} samples)",
                    )
    """

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
                if wrks:
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
        grab_context_manager: t.Optional[t.ContextManager] = None,
    ):
        """Runs the grabber on a sequence.
        NB: `sample_fn` always runs on the main process and may take just the sample
        or the sample and its index.
        NB: `grab_context_manager.__enter__` will be used as `worker_init_fn`,
        please use `GrabberInterface.grab_all_wrk_init` if you want to specify
        your `worker_init_fn`.

        Args:
            sequence: the sequence to grab, usually a SamplesSequence.
            keep_order (bool, optional): if True, `sample_fn` will always receive the
                sample in the correct order. Defaults to False.
            parent_cmd (_type_, optional): the pipelime command running the grabber is
                needed to correctly setup the progress bar. Defaults to None.
            track_message (str, optional): a message shown next to the progress bar.
                Defaults to "".
            sample_fn (_type_, optional): an optional function to run on each grabbed
                element, usually a Sample. The signature may be (sample) or
                (sample, index). Defaults to None.
            size (t.Optional[int], optional): the size of the sequence. If not given,
                `len(sequence)` is evaluated. Defaults to None.
            grab_context_manager (ContextManager, optional): a context manager wrapping
                the whole grabbing operation on the main process. Also,
                `grab_context_manager.__enter__` will be used as `worker_init_fn`.
                Defaults to None.
        """
        from copy import deepcopy

        return self.grab_all_wrk_init(
            sequence=sequence,
            keep_order=keep_order,
            parent_cmd=parent_cmd,
            track_message=track_message,
            sample_fn=sample_fn,
            size=size,
            grab_context_manager=grab_context_manager,
            worker_init_fn=None
            if grab_context_manager is None
            else deepcopy(grab_context_manager).__enter__,
        )

    def grab_all_wrk_init(
        self,
        sequence,
        *,
        keep_order: bool = False,
        parent_cmd=None,
        track_message: str = "",
        sample_fn=None,
        size: t.Optional[int] = None,
        grab_context_manager: t.Optional[t.ContextManager] = None,
        worker_init_fn: t.Union[
            t.Callable, t.Tuple[t.Callable, t.Sequence], None
        ] = None,
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
                element, usually a Sample. The signature may be (sample)
                or (sample, index). Defaults to None.
            size (t.Optional[int], optional): the size of the sequence. If not given,
                `len(sequence)` is evaluated. Defaults to None.
            grab_context_manager (ContextManager, optional): a context manager wrapping
                the whole grabbing operation on the main process. Defaults to None.
            worker_init_fn (optional): a callable or a tuple (callable, args) to run
                before starting the grabbing loop on each worker. If no process is
                spawn, it will be run on the main process. Defaults to None.
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
        grab_all(
            grabber,
            sequence,
            track_fn=track_fn,
            sample_fn=sample_fn,
            size=size,
            grab_context_manager=grab_context_manager,
            worker_init_fn=worker_init_fn,
        )


class InputDatasetInterface(
    PydanticFieldNoDefaultMixin,
    pyd.BaseModel,
    extra="forbid",
    copy_on_model_validation="none",
    allow_population_by_field_name=True,
):
    """Input dataset options.

    Examples:
        How to use it in your command::

            class EasyCommand(PipelimeCommand, title="easy"):
                input: InputDatasetInterface = InputDatasetInterface.pyd_field(
                    alias="i", piper_port=PiperPortType.INPUT
                )
                output: OutputDatasetInterface = OutputDatasetInterface.pyd_field(
                        alias="o", piper_port=PiperPortType.OUTPUT
                )
                grabber: GrabberInterface = GrabberInterface.pyd_field(
                    alias="g"
                )

                def run(self):
                    seq = self.input.create_reader()
                    seq = self.output.append_writer(seq)
                    self.grabber.grab_all(
                        seq,
                        grab_context_manager=self.output.serialization_cm(),
                        keep_order=False,
                        parent_cmd=self,
                        track_message=f"Executing ({len(seq)} samples)",
                    )
    """

    _default_type_description: t.ClassVar[t.Optional[str]] = "The input dataset."
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

        if isinstance(value, (str, bytes, Path)):
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

        reader = SamplesSequence.from_underfolder(
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
        return self.folder.as_posix()


any_serialization_t = t.Literal[
    "CREATE_NEW_FILE", "DEEP_COPY", "SYM_LINK", "HARD_LINK", "REMOTE_FILE"
]
any_item_t = t.Union[None, t.Literal["_"], ItemType]


class SerializationModeInterface(
    pyd.BaseModel,
    extra="forbid",
    copy_on_model_validation="none",
    underscore_attrs_are_private=True,
):
    """Serialization modes for items and keys."""

    override: t.Mapping[
        any_serialization_t, t.Union[any_item_t, t.Sequence[ItemType]]
    ] = pyd.Field(
        default_factory=dict,
        description=(
            "Serialization modes overridden for specific item types, "
            "eg, `{CREATE_NEW_FILE: [ImageItem, my.package.MyItem, "
            "my/module.py:OtherItem]}`. `None` or `_` applies to all items."
        ),
    )
    disable: t.Mapping[
        any_item_t, t.Union[any_serialization_t, t.Sequence[any_serialization_t]]
    ] = pyd.Field(
        default_factory=dict,
        description=(
            "Serialization modes disabled for specific item types, "
            "eg, `{ImageItem: HARD_LINK, my.package.MyItem: [SYM_LINK, DEEP_COPY]}`. "
            "The special key `_` applies to all items."
        ),
    )
    keys: t.Mapping[str, any_serialization_t] = pyd.Field(
        default_factory=dict,
        description=(
            "Serialization modes overridden for specific sample keys, "
            "eg, `{image: CREATE_NEW_FILE}`."
        ),
    )

    _overridden_modes_cms: t.List[t.ContextManager]
    _disabled_modes_cms: t.List[t.ContextManager]

    def _get_class_list(self, cls_paths: t.Union[any_item_t, t.Sequence[ItemType]]):
        if not cls_paths or "_" == cls_paths:
            return []
        if not isinstance(cls_paths, t.Sequence):
            cls_paths = [cls_paths]
        return [c.value for c in cls_paths]  # type: ignore

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

    def get_context_manager(self) -> t.ContextManager:
        from pipelime.utils.context_managers import ContextManagerList

        return ContextManagerList(
            *self._overridden_modes_cms, *self._disabled_modes_cms
        )


class OutputDatasetInterface(
    PydanticFieldNoDefaultMixin,
    pyd.BaseModel,
    extra="forbid",
    copy_on_model_validation="none",
    allow_population_by_field_name=True,
):
    """Output dataset options.

    Examples:
        How to use it in your command::

            class EasyCommand(PipelimeCommand, title="easy"):
                input: InputDatasetInterface = InputDatasetInterface.pyd_field(
                    alias="i", piper_port=PiperPortType.INPUT
                )
                output: OutputDatasetInterface = OutputDatasetInterface.pyd_field(
                        alias="o", piper_port=PiperPortType.OUTPUT
                )
                grabber: GrabberInterface = GrabberInterface.pyd_field(
                    alias="g"
                )

                def run(self):
                    seq = self.input.create_reader()
                    seq = self.output.append_writer(seq)
                    self.grabber.grab_all(
                        seq,
                        grab_context_manager=self.output.serialization_cm(),
                        keep_order=False,
                        parent_cmd=self,
                        track_message=f"Executing ({len(seq)} samples)",
                    )
    """

    _default_type_description: t.ClassVar[t.Optional[str]] = "The output dataset."
    _compact_form: t.ClassVar[
        t.Optional[str]
    ] = "<folder>[,<exists_ok>[,<force_new_files>]]"

    folder: Path = pyd.Field(..., description="Dataset root folder.")
    zfill: t.Optional[pyd.NonNegativeInt] = pyd.Field(
        None, description="Custom index zero-filling."
    )
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

        if isinstance(value, (str, bytes, Path)):
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
        return self.serialization.get_context_manager()

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
        return self.folder.as_posix()


class UrlDataModel(
    pyd.BaseModel,
    extra="forbid",
    copy_on_model_validation="none",
    underscore_attrs_are_private=True,
):
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
    copy_on_model_validation="none",
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


class ToyDatasetInterface(
    pyd.BaseModel, extra="forbid", copy_on_model_validation="none"
):
    """Toy dataset creation options."""

    length: pyd.PositiveInt = pyd.Field(
        ..., description="The number of samples to generate."
    )
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
    image_size: pyd.PositiveInt = pyd.Field(
        256, description="The size of the generated images."
    )
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
    max_labels: pyd.NonNegativeInt = pyd.Field(
        5, description="The maximum number assigned to object labels in the dataset."
    )
    objects_range: t.Tuple[pyd.NonNegativeInt, pyd.NonNegativeInt] = pyd.Field(
        (1, 5), description="The (min, max) number of objects in each sample."
    )
    seed: t.Optional[int] = pyd.Field(None, description="The optional random seed.")

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
            seed=self.seed,
        )


class Interval(PydanticFieldWithDefaultMixin, pyd.BaseModel, extra="forbid"):
    """An interval of indexes, with optional start and stop indices.

    Accepts a single value as ``start`` index, a sequence as ``start`` and ``stop``
    indices or a string ``start:stop``.
    """

    _default_type_description: t.ClassVar[t.Optional[str]] = "An interval of indexes."
    _compact_form: t.ClassVar[t.Optional[str]] = "<start>[:<stop>]"


    start: t.Optional[int] = pyd.Field(
        None,
        description="The first index (included), defaults to the first element (can be negative).",
    )
    stop: t.Optional[int] = pyd.Field(
        None,
        description="The last index (excluded), defaults to the last element (can be negative).",
    )

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value) -> Interval:
        if isinstance(value, cls):
            return value

        if isinstance(value, str):
            value = value.split(":")

        data = {}
        if isinstance(value, int):
            data["start"] = value
        elif isinstance(value, t.Sequence):
            if len(value) > 2:
                raise ValueError(f"Invalid interval: {value}")
            if value[0]:
                data["start"] = int(value[0])  # type: ignore
            if len(value) > 1 and value[1]:
                data["stop"] = int(value[1])  # type: ignore
        elif isinstance(value, t.Mapping):
            data = value
        else:
            raise ValueError(f"Invalid interval: {value}")

        return cls(**data)


class ExtendedInterval(Interval):
    """An interval of indexes, with optional start, stop and step indices.

    Accepts a single value as ``start`` index, a sequence as ``start``, ``stop`` and
    ``step`` indices or a string ``start:stop:step``.
    """

    _compact_form: t.ClassVar[t.Optional[str]] = "<start>[:<stop>[:<step>]]"


    step: t.Optional[int] = pyd.Field(
        None, description="The slice step, defaults to 1 (can be negative)."
    )

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value) -> Interval:
        if isinstance(value, cls):
            return value

        if isinstance(value, str):
            value = value.split(":")

        data = {}
        if isinstance(value, int):
            data["start"] = value
        elif isinstance(value, t.Sequence):
            if len(value) > 3:
                raise ValueError(f"Invalid interval: {value}")
            if value[0]:
                data["start"] = int(value[0])  # type: ignore
            if len(value) > 1 and value[1]:
                data["stop"] = int(value[1])  # type: ignore
            if len(value) > 2 and value[2]:
                data["step"] = int(value[2])  # type: ignore
        elif isinstance(value, t.Mapping):
            data = value
        else:
            raise ValueError(f"Invalid interval: {value}")

        return cls(**data)
