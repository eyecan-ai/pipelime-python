import typing as t
import pydantic as pyd

import pipelime.commands.interfaces as pl_interfaces
from pipelime.piper import PipelimeCommand, PiperPortType
import pipelime.utils.pydantic_types as pl_types


class TimeItCommand(PipelimeCommand, title="timeit"):
    """Measures the average time to get a sample from a sequence."""

    class OutputTime(pyd.BaseModel):
        nanosec: int

        def __repr__(self) -> str:
            return self.__piper_repr__()

        def __piper_repr__(self) -> str:
            from pipelime.cli.utils import time_to_str

            return time_to_str(self.nanosec)

    input: t.Optional[
        pl_interfaces.InputDatasetInterface
    ] = pl_interfaces.InputDatasetInterface.pyd_field(
        alias="i",
        is_required=False,
        description=(
            "The input dataset. If None, the first operation "
            "must be a sequence generator."
        ),
        piper_port=PiperPortType.INPUT,
    )

    output: t.Optional[
        pl_interfaces.OutputDatasetInterface
    ] = pl_interfaces.OutputDatasetInterface.pyd_field(
        alias="o", is_required=False, piper_port=PiperPortType.OUTPUT
    )

    operations: t.Optional[pl_types.YamlInput] = pyd.Field(
        None,
        alias="op",
        description=(
            "An optional pipeline to run or a path to a yaml/json file as "
            "<filepath>[:<key-path>]\n"
            "The pipeline is defined as a mapping or a sequence of mappings where "
            "each key is a samples sequence operator to run, eg, `map`, `sort`, etc., "
            "while the value gathers the arguments, ie, a single value, a sequence of "
            "values or a keyword mapping."
        ),
    )

    skip_first: pyd.NonNegativeInt = pyd.Field(
        1, alias="s", description="Skip the first n samples."
    )
    max_samples: t.Optional[pyd.PositiveInt] = pyd.Field(
        None,
        alias="m",
        description="Grab at most `max_samples` and take the average time.",
    )
    repeat: pyd.NonNegativeInt = pyd.Field(
        0, alias="r", description="Repeat the measurement `repeat` times."
    )
    process: bool = pyd.Field(
        False,
        alias="p",
        description=(
            "Measure process time instead of using a performance counter clock."
        ),
    )

    average_time: t.Optional[OutputTime] = pyd.Field(
        None,
        description="The average time to get a sample from the sequence.",
        exclude=True,
        repr=False,
        piper_port=PiperPortType.OUTPUT,
    )

    def run(self):
        import time
        from pipelime.sequences import build_pipe, SamplesSequence

        if self.input is None and self.operations is None:
            raise ValueError("No input dataset or operation defined.")

        clock_fn = time.process_time_ns if self.process else time.perf_counter_ns

        elapsed_times = []
        for r in range(self.repeat + 1):
            seq = SamplesSequence if self.input is None else self.input.create_reader()
            if self.operations is not None:
                seq = build_pipe(self.operations.value, seq)  # type: ignore

            assert isinstance(seq, SamplesSequence)
            if self.output is not None:
                seq = self.output.append_writer(seq)

            seqit = iter(seq)
            for s in range(self.skip_first):
                _ = next(seqit)

            if self.max_samples is None:
                start = clock_fn()
                for _ in seqit:
                    pass
                end = clock_fn()
            else:
                start = clock_fn()
                for s in range(self.max_samples):
                    _ = next(seqit)
                end = clock_fn()
            elapsed_times.append(end - start)

        self.average_time = TimeItCommand.OutputTime(
            nanosec=int(sum(elapsed_times) // len(elapsed_times))
        )


class PipeCommand(PipelimeCommand, title="pipe"):
    """A general-purpose command to build up linear pipelines."""

    operations: pl_types.YamlInput = pyd.Field(
        ...,
        alias="op",
        description=(
            "The pipeline to run or a path to a yaml/json file as "
            "<filepath>[:<key-path>]\n"
            "The pipeline is defined as a mapping or a sequence of mappings where "
            "each key is a samples sequence operator to run, eg, `map`, `sort`, etc., "
            "while the value gathers the arguments, ie, a single value, a sequence of "
            "values or a keyword mapping."
        ),
    )

    input: t.Optional[
        pl_interfaces.InputDatasetInterface
    ] = pl_interfaces.InputDatasetInterface.pyd_field(
        alias="i",
        is_required=False,
        description=(
            "The input dataset. If None, the first operation "
            "must be a sequence generator."
        ),
        piper_port=PiperPortType.INPUT,
    )

    output: pl_interfaces.OutputDatasetInterface = (
        pl_interfaces.OutputDatasetInterface.pyd_field(
            alias="o", piper_port=PiperPortType.OUTPUT
        )
    )

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )

    def __init__(self, **data):
        super().__init__(**data)
        if not self.operations:
            raise ValueError(f"Invalid pipeline: {self.operations}")

    def run(self):
        from pipelime.sequences import build_pipe, SamplesSequence

        seq = SamplesSequence if self.input is None else self.input.create_reader()
        seq = build_pipe(self.operations.value, seq)  # type: ignore
        seq = self.output.append_writer(seq)

        self.grabber.grab_all(
            seq,
            grab_context_manager=self.output.serialization_cm(),
            keep_order=False,
            parent_cmd=self,
            track_message=f"Writing results ({len(seq)} samples)",
        )


class CloneCommand(PipelimeCommand, title="clone"):
    """Clone a dataset. You can use this command to create a local copy of a dataset
    hosted on a remote data lake by disabling the `REMOTE_FILE` serialization option."""

    input: pl_interfaces.InputDatasetInterface = (
        pl_interfaces.InputDatasetInterface.pyd_field(
            alias="i", piper_port=PiperPortType.INPUT
        )
    )

    output: pl_interfaces.OutputDatasetInterface = (
        pl_interfaces.OutputDatasetInterface.pyd_field(
            alias="o", piper_port=PiperPortType.OUTPUT
        )
    )

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
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
            track_message=f"Cloning data ({len(seq)} samples)",
        )


class ConcatCommand(PipelimeCommand, title="cat"):
    """Concatenate two or more datasets."""

    inputs: t.Union[
        pl_interfaces.InputDatasetInterface,
        t.Sequence[pl_interfaces.InputDatasetInterface],
    ] = pl_interfaces.InputDatasetInterface.pyd_field(
        alias="i", piper_port=PiperPortType.INPUT
    )

    output: pl_interfaces.OutputDatasetInterface = (
        pl_interfaces.OutputDatasetInterface.pyd_field(
            alias="o", piper_port=PiperPortType.OUTPUT
        )
    )

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )

    def run(self):
        inputs = self.inputs if isinstance(self.inputs, t.Sequence) else [self.inputs]
        input_it = iter(inputs)
        seq = next(input_it).create_reader()
        for input_ in input_it:
            seq = seq.cat(input_.create_reader())
        self.grabber.grab_all(
            self.output.append_writer(seq),
            grab_context_manager=self.output.serialization_cm(),
            keep_order=False,
            parent_cmd=self,
            track_message=f"Writing data ({len(seq)} samples)",
        )


class ZipCommand(PipelimeCommand, title="zip"):
    """Zip two or more datasets merging items."""

    inputs: t.Union[
        pl_interfaces.InputDatasetInterface,
        t.Sequence[pl_interfaces.InputDatasetInterface],
    ] = pl_interfaces.InputDatasetInterface.pyd_field(
        alias="i", piper_port=PiperPortType.INPUT
    )

    output: pl_interfaces.OutputDatasetInterface = (
        pl_interfaces.OutputDatasetInterface.pyd_field(
            alias="o", piper_port=PiperPortType.OUTPUT
        )
    )

    key_format: t.Union[str, t.Sequence[str]] = pyd.Field(
        "*",
        description=(
            "The zipped samples' key format. Any `*` will be replaced with the "
            "source key, eg, `my_*_key` on [`image`, `mask`] generates "
            "`my_image_key` and `my_mask_key`. If no `*` is found, the string is "
            "suffixed to source key, ie, `MyKey` on `image` gives "
            "`imageMyKey`. If empty, the source key will be used as-is."
        ),
    )

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )

    def run(self):
        inputs = self.inputs if isinstance(self.inputs, t.Sequence) else [self.inputs]

        key_formats = [self.key_format]*len(inputs) if isinstance(self.key_format, str) else self.key_format
        if len(key_formats) != len(inputs):
            raise ValueError(f"Number of inputs and key formats do not match.")

        input_it = iter(inputs)
        seq = next(input_it).create_reader()
        for input_, kf in zip(input_it, key_formats):
            seq = seq.zip(input_.create_reader(), key_format=kf)
        self.grabber.grab_all(
            self.output.append_writer(seq),
            grab_context_manager=self.output.serialization_cm(),
            keep_order=False,
            parent_cmd=self,
            track_message=f"Zipping data ({len(seq)} samples)",
        )


class AddRemoteCommand(PipelimeCommand, title="remote-add"):
    """Upload samples to one or more remotes.
    Slicing options filter the samples to upload,
    but the whole dataset is always written out.
    """

    input: pl_interfaces.InputDatasetInterface = (
        pl_interfaces.InputDatasetInterface.pyd_field(
            alias="i", piper_port=PiperPortType.INPUT
        )
    )

    remotes: t.Union[
        pl_interfaces.RemoteInterface, t.Sequence[pl_interfaces.RemoteInterface]
    ] = pl_interfaces.RemoteInterface.pyd_field(alias="r")

    keys: t.Union[str, t.Sequence[str]] = pyd.Field(
        default_factory=list,
        alias="k",
        description="Keys to upload. Leave empty to upload all the keys.",
    )

    start: t.Optional[int] = pyd.Field(
        None, description="The first sample (included), defaults to the first element."
    )
    stop: t.Optional[int] = pyd.Field(
        None, description="The last sample (excluded), defaults to the whole sequence."
    )
    step: t.Optional[int] = pyd.Field(
        None, description="The slice step, defaults to 1."
    )

    output: t.Optional[
        pl_interfaces.OutputDatasetInterface
    ] = pl_interfaces.OutputDatasetInterface.pyd_field(
        alias="o",
        is_required=False,
        description="Optional output dataset with remote items.",
        piper_port=PiperPortType.OUTPUT,
    )

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )

    @pyd.validator("remotes", "keys")
    def validate_remotes(cls, v):
        return (
            v if not isinstance(v, (str, bytes)) and isinstance(v, t.Sequence) else [v]
        )

    def run(self):
        from pipelime.stages import StageUploadToRemote

        original = self.input.create_reader()
        seq = original.slice(start=self.start, stop=self.stop, step=self.step).map(
            StageUploadToRemote(
                remotes=[r.get_url() for r in self.remotes],  # type: ignore
                keys_to_upload=self.keys,
            )
        )

        if self.output is None:
            self._grab_all(seq, None)
        else:
            # NB: we should always write the whole dataset,
            # even if we are uploading only a slice
            if self.start is None and self.stop is None and self.step is None:
                self._grab_all(
                    self.output.append_writer(seq), self.output.serialization_cm()
                )
            else:
                self._grab_all(seq, None)
                self._grab_all(
                    self.output.append_writer(original), self.output.serialization_cm()
                )

    def _grab_all(self, seq, sm):
        self.grabber.grab_all(
            seq,
            grab_context_manager=sm,
            keep_order=False,
            parent_cmd=self,
            track_message=f"Uploading data ({len(seq)} samples)",
        )


class RemoveRemoteCommand(PipelimeCommand, title="remote-remove"):
    """Remove one or more remote from a dataset.
    NB: data is not removed from the remote data lake."""

    input: pl_interfaces.InputDatasetInterface = (
        pl_interfaces.InputDatasetInterface.pyd_field(
            alias="i", piper_port=PiperPortType.INPUT
        )
    )

    remotes: t.Union[
        pl_interfaces.RemoteInterface, t.Sequence[pl_interfaces.RemoteInterface]
    ] = pl_interfaces.RemoteInterface.pyd_field(alias="r")

    keys: t.Union[str, t.Sequence[str]] = pyd.Field(
        default_factory=list,
        alias="k",
        description=(
            "Remove remotes on these keys only. Leave empty to affect all the keys."
        ),
    )

    output: pl_interfaces.OutputDatasetInterface = (
        pl_interfaces.OutputDatasetInterface.pyd_field(
            alias="o", piper_port=PiperPortType.OUTPUT
        )
    )

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )

    @pyd.validator("remotes", "keys")
    def validate_remotes(cls, v):
        return (
            v if not isinstance(v, (str, bytes)) and isinstance(v, t.Sequence) else [v]
        )

    def run(self):
        from pipelime.stages import StageForgetSource

        remotes = [r.get_url() for r in self.remotes]  # type: ignore
        remove_all = remotes if not self.keys else []
        remove_by_key = {k: remotes for k in self.keys}

        seq = self.input.create_reader().map(
            StageForgetSource(*remove_all, **remove_by_key)
        )
        self.grabber.grab_all(
            self.output.append_writer(seq),
            grab_context_manager=self.output.serialization_cm(),
            keep_order=False,
            parent_cmd=self,
            track_message=f"Removing remotes ({len(seq)} samples)",
        )


class ValidateCommand(PipelimeCommand, title="validate"):
    """Outputs a minimal schema which will validate the given input."""

    class OutputSchemaDefinition(pyd.BaseModel):
        schema_def: t.Any

        def __repr__(self) -> str:
            return self.__piper_repr__()

        def __piper_repr__(self) -> str:
            import yaml

            return yaml.safe_dump(self.schema_def, sort_keys=False)

    class OutputCmdLineSchema(pyd.BaseModel):
        schema_def: t.Any

        def __repr__(self) -> str:
            return self.__piper_repr__()

        def __piper_repr__(self) -> str:
            return " ".join(self._flatten_dict(self.schema_def))

        def _flatten_dict(self, dict_, parent_key="", sep=".", prefix="--"):
            cmd_line = []
            for k, v in dict_.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else f"{prefix}{k}"
                if isinstance(v, t.Mapping):
                    cmd_line.extend(
                        self._flatten_dict(
                            v, parent_key=new_key, sep=sep, prefix=prefix
                        )
                    )
                elif isinstance(v, t.Sequence) and not isinstance(v, (str, bytes)):
                    cmd_line.extend(
                        self._flatten_list(
                            v, parent_key=new_key, sep=sep, prefix=prefix
                        )
                    )
                else:
                    cmd_line.append(new_key)
                    cmd_line.append(str(v))
            return cmd_line

        def _flatten_list(self, list_, parent_key, sep=".", prefix="--"):
            cmd_line = []
            for i, v in enumerate(list_):
                new_key = f"{parent_key}[{i}]"
                if isinstance(v, t.Mapping):
                    cmd_line.extend(
                        self._flatten_dict(
                            v, parent_key=new_key, sep=sep, prefix=prefix
                        )
                    )
                elif isinstance(v, t.Sequence) and not isinstance(v, (str, bytes)):
                    cmd_line.extend(
                        self._flatten_list(
                            v, parent_key=new_key, sep=sep, prefix=prefix
                        )
                    )
                else:
                    cmd_line.append(new_key)
                    cmd_line.append(str(v))
            return cmd_line

    input: pl_interfaces.InputDatasetInterface = (
        pl_interfaces.InputDatasetInterface.pyd_field(
            alias="i", piper_port=PiperPortType.INPUT
        )
    )

    max_samples: int = pyd.Field(
        0,
        alias="m",
        description=(
            "Max number of samples to consider when creating the schema.  "
            "Set to 0 to check all the samples."
        ),
    )
    root_key_path: str = pyd.Field(
        "input.schema", alias="r", description="Root key path for the output schema."
    )

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )

    output_schema_def: t.Optional[OutputSchemaDefinition] = pyd.Field(
        None,
        description="yaml schema definition",
        exclude=True,
        repr=False,
        piper_port=PiperPortType.OUTPUT,
    )
    output_cmd_line_schema: t.Optional[OutputCmdLineSchema] = pyd.Field(
        None,
        description="Schema definition on command line",
        exclude=True,
        repr=False,
        piper_port=PiperPortType.OUTPUT,
    )

    def run(self):
        import json
        from pipelime.stages import StageItemInfo

        seq = self.input.create_reader()
        if self.max_samples != 0:
            seq = seq[0 : self.max_samples]  # noqa
        item_info = StageItemInfo()

        self.grabber.grab_all(
            seq,
            keep_order=False,
            parent_cmd=self,
            track_message=f"Reading data ({len(seq)} samples)",
            sample_fn=item_info,
        )

        sample_schema = {
            k: pl_interfaces.ItemValidationModel(
                class_path=info.item_type,
                is_optional=(info.count_ != len(seq)),
                is_shared=info.is_shared,
            ).dict(by_alias=True)
            for k, info in item_info.items_info().items()
        }

        sample_validation = pl_interfaces.SampleValidationInterface(
            sample_schema=sample_schema,
            ignore_extra_keys=False,
            lazy=(self.max_samples == 0),
            max_samples=self.max_samples,
        ).dict()

        def _type2str(data):
            import inspect
            from pipelime.items import Item
            from pipelime.utils.pydantic_types import ItemType

            for k, v in data.items():
                if inspect.isclass(v) and issubclass(v, Item):
                    data[k] = str(ItemType(__root__=v))
                elif isinstance(v, t.Mapping):
                    _type2str(v)
            return data

        sample_validation = _type2str(sample_validation)

        if self.root_key_path:
            import pydash as py_

            tmp_dict = {}
            py_.set_(tmp_dict, self.root_key_path, sample_validation)
            sample_validation = tmp_dict

        self.output_schema_def = ValidateCommand.OutputSchemaDefinition(
            schema_def=sample_validation
        )
        self.output_cmd_line_schema = ValidateCommand.OutputCmdLineSchema(
            schema_def=sample_validation
        )


class MapCommand(PipelimeCommand, title="map"):
    """Apply a stage on a dataset."""

    stage: t.Union[str, t.Mapping[str, t.Mapping[str, t.Any]]] = pyd.Field(
        ...,
        alias="s",
        description=(
            "A stage to apply. Can be a stage name/class_path (with no arguments) or "
            "a dictionary with the stage name/class_path as key and the arguments "
            "passed by keywords."
        ),
    )

    input: pl_interfaces.InputDatasetInterface = (
        pl_interfaces.InputDatasetInterface.pyd_field(
            alias="i", piper_port=PiperPortType.INPUT
        )
    )

    output: pl_interfaces.OutputDatasetInterface = (
        pl_interfaces.OutputDatasetInterface.pyd_field(
            alias="o", piper_port=PiperPortType.OUTPUT
        )
    )

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )

    def run(self):
        seq = self.input.create_reader()
        seq = seq.map(
            self.stage if isinstance(self.stage, t.Mapping) else {self.stage: {}}
        )
        self.grabber.grab_all(
            self.output.append_writer(seq),
            grab_context_manager=self.output.serialization_cm(),
            keep_order=False,
            parent_cmd=self,
            track_message=f"Mapping data ({len(seq)} samples)",
        )


class SortCommand(PipelimeCommand, title="sort"):
    """Sort a dataset based on metadata values."""

    key_path: str = pyd.Field(
        ...,
        alias="k",
        description=(
            "A pydash-like key path. The path is built by splitting the mapping "
            "keys by `.` and enclosing list indexes within `[]`. "
            "Use `\\` to escape the `.` character."
        ),
    )

    input: pl_interfaces.InputDatasetInterface = (
        pl_interfaces.InputDatasetInterface.pyd_field(
            alias="i", piper_port=PiperPortType.INPUT
        )
    )

    output: pl_interfaces.OutputDatasetInterface = (
        pl_interfaces.OutputDatasetInterface.pyd_field(
            alias="o", piper_port=PiperPortType.OUTPUT
        )
    )

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )

    def run(self):
        def _sort_key_fn(x):
            return x.deep_get(self.key_path)

        seq = self.input.create_reader()
        seq = seq.sort(_sort_key_fn)
        self.grabber.grab_all(
            self.output.append_writer(seq),
            grab_context_manager=self.output.serialization_cm(),
            keep_order=False,
            parent_cmd=self,
            track_message=f"Sorting data ({len(seq)} samples)",
        )
