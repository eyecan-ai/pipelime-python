from __future__ import annotations

import json
import typing as t
import uuid
from pathlib import Path

import pydantic.v1 as pyd
from pydantic.v1.generics import GenericModel

from pipelime.piper import PiperPortType
from pipelime.utils.pydantic_types import ItemType, SampleValidationInterface, YamlInput


class PydanticFieldMixinBase:
    # override in derived clasess
    _default_type_description: t.ClassVar[t.Optional[str]] = None
    _compact_form: t.ClassVar[t.Optional[str]] = None
    _default_port_type: t.ClassVar[PiperPortType] = PiperPortType.PARAMETER

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
    def pyd_field(
        cls,
        *,
        description: t.Optional[str] = None,
        piper_port: t.Optional[PiperPortType] = None,
        **kwargs,
    ):
        return pyd.Field(
            default_factory=cls,  # type: ignore
            description=cls._description(description),  # type: ignore
            piper_port=piper_port or cls._default_port_type,
            **kwargs,
        )


class PydanticFieldNoDefaultMixin(PydanticFieldMixinBase):
    @classmethod
    def pyd_field(
        cls,
        *,
        is_required: bool = True,
        description: t.Optional[str] = None,
        piper_port: t.Optional[PiperPortType] = None,
        **kwargs,
    ):
        return pyd.Field(
            ... if is_required else None,
            description=cls._description(description),  # type: ignore
            piper_port=piper_port or cls._default_port_type,
            **kwargs,
        )


class GrabberInterface(PydanticFieldWithDefaultMixin, pyd.BaseModel, extra="forbid"):
    """Multiprocessing grabbing options.

    Examples:
        How to use it in your command::

            class EasyCommand(PipelimeCommand, title="easy"):
                input: InputDatasetInterface = InputDatasetInterface.pyd_field(alias="i")
                output: OutputDatasetInterface = OutputDatasetInterface.pyd_field(alias="o")
                grabber: GrabberInterface = GrabberInterface.pyd_field(alias="g")

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
    _compact_form: t.ClassVar[t.Optional[str]] = (
        "<num_workers>[,<prefetch>[,<allow_nested_mp>]]"
    )

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
    allow_nested_mp: bool = pyd.Field(
        False, description="Whether to allow nested multiprocessing."
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
                raw_data = str(value).split(",")
                try:
                    if raw_data[0]:
                        data["num_workers"] = int(raw_data[0])
                    if len(raw_data) > 1 and raw_data[1]:
                        data["prefetch"] = int(raw_data[1])
                    if len(raw_data) > 2 and raw_data[2]:
                        data["allow_nested_mp"] = raw_data[2].lower() == "true"
                except ValueError:
                    raise ValueError("Invalid grabber definition.")
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
            worker_init_fn=(
                None
                if grab_context_manager is None
                else deepcopy(grab_context_manager).__enter__
            ),
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
            num_workers=self.num_workers,
            prefetch=self.prefetch,
            keep_order=keep_order,
            allow_nested_mp=self.allow_nested_mp,
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
                input: InputDatasetInterface = InputDatasetInterface.pyd_field(alias="i")
                output: OutputDatasetInterface = OutputDatasetInterface.pyd_field(alias="o")
                grabber: GrabberInterface = GrabberInterface.pyd_field(alias="g")

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
    _default_port_type: t.ClassVar[PiperPortType] = PiperPortType.INPUT

    folder: t.Optional[Path] = pyd.Field(
        None,
        description=(
            "Dataset root folder. Either `folder` is not `None` or the "
            "`pipe` starts with a generator sequence."
        ),
    )
    merge_root_items: bool = pyd.Field(
        True,
        description=(
            "Adds root items as shared items "
            "to each sample (sample values take precedence)."
        ),
    )
    skip_empty: bool = pyd.Field(False, description="Filter out empty samples.")
    pipe: t.Optional[YamlInput] = pyd.Field(
        None,
        description=(
            "The pipeline to run or a path to a yaml/json file as "
            "<filepath>[:<key-path>]. Either `folder` is not `None` or the "
            "`pipe` starts with a generator sequence.\n"
            "The pipeline is defined as a mapping or a sequence of mappings where "
            "each key is a samples sequence operator to run, eg, `map`, `sort`, etc., "
            "while the value gathers the arguments, ie, a single value, a sequence of "
            "values or a keyword mapping."
        ),
    )
    schema_: t.Optional[SampleValidationInterface] = pyd.Field(
        None,
        alias="schema",
        description="Sample schema validation, verified after all operations.",
    )

    @pyd.validator("folder")
    def resolve_folder(cls, v: t.Optional[Path]):
        if v:
            # see https://bugs.python.org/issue38671
            return v.resolve().absolute()
        return v

    @pyd.validator("pipe", always=True)
    def check_pipe_and_folder(
        cls, v: t.Optional[YamlInput], values: t.Mapping[str, t.Any]
    ):
        if v is None:
            if values.get("folder", None) is None:
                raise ValueError("Either `folder` or `pipe` (or both) must be defined.")
        elif not v.value or not isinstance(v.value, (t.Mapping, t.Sequence)):
            raise ValueError(f"Invalid pipeline: {v.value}")
        return v

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

    @staticmethod
    def is_empty_fn(x):
        return not all(i.is_shared for i in x.values())

    def create_reader(self):
        from pipelime.sequences import SamplesSequence, build_pipe

        if self.folder is None:
            reader = build_pipe(self.pipe.value, SamplesSequence)  # type: ignore
        else:
            reader = SamplesSequence.from_underfolder(
                folder=self.folder,
                merge_root_items=self.merge_root_items,
                must_exist=True,
            )

        if self.skip_empty:
            reader = reader.filter(InputDatasetInterface.is_empty_fn)  # type: ignore

        if self.pipe is not None and self.folder is not None:
            reader = build_pipe(self.pipe.value, reader)  # type: ignore

        if self.schema_ is not None:
            reader = self.schema_.append_validator(reader)
        return reader

    def __repr__(self) -> str:
        return self.__piper_repr__()

    def __piper_repr__(self) -> str:
        return (
            self.folder.as_posix() if self.folder else f"<generator-{uuid.uuid1().hex}>"
        )


IDataset = InputDatasetInterface


any_serialization_t = t.Literal["CREATE_NEW_FILE", "DEEP_COPY", "SYM_LINK", "HARD_LINK"]
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
                input: InputDatasetInterface = InputDatasetInterface.pyd_field(alias="i")
                output: OutputDatasetInterface = OutputDatasetInterface.pyd_field(alias="o")
                grabber: GrabberInterface = GrabberInterface.pyd_field(alias="g")

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
    _compact_form: t.ClassVar[t.Optional[str]] = (
        "<folder>[,<exists_ok>[,<force_new_files>]]"
    )
    _default_port_type: t.ClassVar[PiperPortType] = PiperPortType.OUTPUT

    folder: t.Optional[Path] = pyd.Field(
        None,
        description="Dataset root folder. Input `folder` and/or `pipe` must be set.",
    )
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
    pipe: t.Optional[YamlInput] = pyd.Field(
        None,
        description=(
            "The pipeline to run or a path to a yaml/json file as "
            "<filepath>[:<key-path>]. It cannot start with a generator sequence. "
            "Input `folder` and/or `pipe` must be set.\n"
            "The pipeline is defined as a mapping or a sequence of mappings where "
            "each key is a samples sequence operator to run, eg, `map`, `sort`, etc., "
            "while the value gathers the arguments, ie, a single value, a sequence of "
            "values or a keyword mapping."
        ),
    )
    schema_: t.Optional[SampleValidationInterface] = pyd.Field(
        None,
        alias="schema",
        description="Sample schema validation, verified before any other operation.",
    )

    @pyd.validator("folder")
    def resolve_folder(cls, v: t.Optional[Path]):
        if v:
            # see https://bugs.python.org/issue38671
            return v.resolve().absolute()
        return v

    @pyd.validator("exists_ok", always=True)
    def _check_folder_exists(cls, v: bool, values: t.Mapping[str, t.Any]) -> bool:
        if (
            not v
            and values.get("folder", None) is not None
            and values["folder"].exists()
        ):
            raise ValueError(
                f"Trying to overwrite an existing dataset: `{values['folder']}`. "
                "Please use `exists_ok=True` to overwrite."
            )
        return v

    @pyd.validator("pipe", always=True)
    def check_pipe_and_folder(
        cls, v: t.Optional[YamlInput], values: t.Mapping[str, t.Any]
    ):
        if v is None:
            if values.get("folder", None) is None:
                raise ValueError("Either `folder` or `pipe` (or both) must be defined.")
        elif not v.value or not isinstance(v.value, (t.Mapping, t.Sequence)):
            raise ValueError(f"Invalid pipeline: {v.value}")
        return v

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
        from pipelime.sequences import build_pipe

        if self.schema_ is not None:
            sequence = self.schema_.append_validator(sequence)
        if self.pipe is not None:
            sequence = build_pipe(self.pipe.value, sequence)  # type: ignore
        if self.folder is not None:
            sequence = sequence.to_underfolder(
                folder=self.folder,
                zfill=self.zfill,
                exists_ok=self.exists_ok,
                key_serialization_mode=self.serialization.keys,
            )

        return sequence

    def as_pipe(self):
        pipe_list = []
        if self.schema_ is not None:
            pipe_list.append(self.schema_.as_pipe())
        if self.pipe is not None:
            if isinstance(self.pipe.value, t.Mapping):
                pipe_list.append(self.pipe.value)
            else:
                pipe_list.extend(self.pipe.value)  # type: ignore
        if self.folder is not None:
            pipe_list.append(
                {
                    "to_underfolder": {
                        "folder": self.folder,
                        "zfill": self.zfill,
                        "exists_ok": self.exists_ok,
                        "key_serialization_mode": self.serialization.keys,
                    }
                }
            )
        return pipe_list

    def as_input(self, **kwargs):
        """Returns an input dataset interface reading the same folder.
        All other arguments are passed to the constructor.
        """
        return InputDatasetInterface(folder=self.folder, **kwargs)

    def __repr__(self) -> str:
        return self.__piper_repr__()

    def __piper_repr__(self) -> str:
        return self.folder.as_posix() if self.folder else f"<pipe-{uuid.uuid1().hex}>"


ODataset = OutputDatasetInterface


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
    _compact_form: t.ClassVar[t.Optional[str]] = "[<start>][:<stop>]"

    start: t.Optional[int] = pyd.Field(
        None,
        description=(
            "The first index (included), defaults to the first element "
            "(can be negative)."
        ),
    )
    stop: t.Optional[int] = pyd.Field(
        None,
        description=(
            "The last index (excluded), defaults to the last element (can be negative)."
        ),
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

    _compact_form: t.ClassVar[t.Optional[str]] = "[<start>][:<stop>][:<step>]"

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


ValueType = t.TypeVar("ValueType")


class OutputValueInterface(
    PydanticFieldNoDefaultMixin,
    GenericModel,
    t.Generic[ValueType],
    extra="forbid",
    copy_on_model_validation="none",
    underscore_attrs_are_private=True,
):
    """Interface that allows to store a value in a file.

    Examples:
        How to use it in your command::

        class EasyCommand(PipelimeCommand, title="easy"):
            output_value: OutputValueInterface[int] = OutputValueInterface.pyd_field()

            def run(self):
                # this will write the value 42 to the given file
                self.output_value.set(42)
    """

    _default_type_description: t.ClassVar[t.Optional[str]] = "An output value."
    _compact_form: t.ClassVar[t.Optional[str]] = "<file>[,<exists_ok>]"
    _default_port_type: t.ClassVar[PiperPortType] = PiperPortType.OUTPUT
    _data: ValueType

    file: Path = pyd.Field(
        description=(
            "The output file that will store the value. If the file exists, it will"
            "be overwritten if `exists_ok` is set to `True`."
        ),
    )
    exists_ok: bool = pyd.Field(
        False,
        description=(
            "If `True`, the output file will be overwritten if it exists. "
            "If `False`, an error will be raised."
        ),
    )

    @classmethod
    def validate(cls, value):
        if isinstance(value, OutputValueInterface):
            return value

        if isinstance(value, (str, bytes, Path)):
            data = {}
            raw_data = str(value).split(",")
            data["file"] = raw_data[0]
            if len(raw_data) > 1:
                # NB: the value is left as-is when it is not True nor False
                # to raise an error on validation
                data["exists_ok"] = (
                    True
                    if raw_data[1].lower() == "true"
                    else (False if raw_data[1].lower() == "false" else raw_data[1])
                )
            value = data

        if isinstance(value, t.Mapping):
            return OutputValueInterface(**value)

        raise ValueError("Invalid OutputValueInterface definition.")

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @pyd.validator("exists_ok", always=True)
    def _check_file_exists(cls, v: bool, values: t.Mapping[str, t.Any]) -> bool:
        if (v is False) and (values["file"].exists()):
            raise ValueError(
                f"Trying to overwrite an existing file: `{values['file']}`. "
                "Please use `exists_ok=True` to overwrite."
            )
        return v

    def get(self) -> ValueType:
        """Return the stored value."""
        return self._data

    def set(self, value: ValueType) -> None:
        """Writes the value to the output file.

        Args:
            value: The value to write.
        """
        self._data = value
        with open(self.file, "w") as f:
            json.dump(self._data, f)
