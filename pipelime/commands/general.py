import typing as t

import pydantic as pyd

import pipelime.commands.interfaces as pl_interfaces
import pipelime.sequences as pls
import pipelime.utils.pydantic_types as pl_types
from pipelime.piper import PipelimeCommand, PiperPortType


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
        1, alias="s", description="Skip the first n samples, then start the timer."
    )
    max_samples: t.Optional[pyd.PositiveInt] = pyd.Field(
        None,
        alias="m",
        description="Grab at most `max_samples` and take the average time.",
    )
    repeat: pyd.PositiveInt = pyd.Field(
        1, alias="r", description="Repeat the measurement `repeat` times."
    )
    process: bool = pyd.Field(
        False,
        alias="p",
        description=(
            "Measure process time instead of using a performance counter clock."
        ),
    )
    clear_output_folder: bool = pyd.Field(
        True, alias="c", description="Remove the output folder before each run."
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
        import shutil
        from pipelime.sequences import build_pipe, SamplesSequence

        if self.input is None and self.operations is None:
            raise ValueError("No input dataset or operation defined.")

        clock_fn = time.process_time_ns if self.process else time.perf_counter_ns

        elapsed_times = []
        num_samples = 0
        for r in range(self.repeat):
            seq = SamplesSequence if self.input is None else self.input.create_reader()
            if self.operations is not None:
                seq = build_pipe(self.operations.value, seq)  # type: ignore
            assert isinstance(seq, SamplesSequence)

            if self.output is not None:
                if self.clear_output_folder:
                    shutil.rmtree(
                        self.output.folder.resolve().absolute().as_posix(),
                        ignore_errors=True,
                    )
                seq = self.output.append_writer(seq)

            seqit = iter(seq)
            for s in range(self.skip_first):
                _ = next(seqit)

            available_samples = len(seq) - self.skip_first
            if self.max_samples is None:
                start = clock_fn()
                for _ in seqit:
                    pass
                end = clock_fn()
            else:
                available_samples = min(self.max_samples, available_samples)
                start = clock_fn()
                for s in range(available_samples):
                    _ = next(seqit)
                end = clock_fn()
            num_samples += available_samples
            elapsed_times.append(end - start)

        self.average_time = TimeItCommand.OutputTime(
            nanosec=int(sum(elapsed_times) // num_samples)
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

    @pyd.validator("operations")
    def _validate_operations(cls, v: pl_types.YamlInput) -> pl_types.YamlInput:
        if not v.value or not isinstance(v.value, (t.Mapping, t.Sequence)):
            raise ValueError(f"Invalid pipeline: {v.value}")
        return v

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
    """Clone a dataset.

    You can use this command to create a local copy of a dataset
    hosted on a remote data lake by disabling the `REMOTE_FILE` serialization option.
    """

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
            "The zipped samples' key format FOR EACH INPUT SEQUENCE EXCEPT THE FIRST "
            "ONE. Any `*` will be replaced with the source key, eg, `my_*_key` on "
            "[`image`, `mask`] generates `my_image_key` and `my_mask_key`. If no `*` "
            "is found, the string is suffixed to source key, ie, `MyKey` on `image` "
            "gives `imageMyKey`. If empty, the source key will be used as-is."
        ),
    )

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )

    def run(self):
        inputs = self.inputs if isinstance(self.inputs, t.Sequence) else [self.inputs]

        key_formats = (
            [self.key_format] * (len(inputs) - 1)
            if isinstance(self.key_format, str)
            else self.key_format
        )
        if len(key_formats) != (len(inputs) - 1):
            raise ValueError(
                f"{len(key_formats)} key format string are provided, "
                f"but {len(inputs)} are required."
            )

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


class RemoteCommandBase(PipelimeCommand):
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
        description="Affected keys. Leave empty to take all the keys.",
    )

    start: int = pyd.Field(
        0,
        description=(
            "The first sample (included), defaults to the first element. "
            "Can be negative, in which case it counts from the end."
        ),
    )
    stop: t.Optional[int] = pyd.Field(
        None,
        description=(
            "The last sample (excluded), defaults to the whole sequence."
            "Can be negative, in which case it counts from the end."
        ),
    )
    step: int = pyd.Field(
        1, description="The slice step, defaults to 1. Can be negative."
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

    def _run_remote_op(self, stage, message):
        from pipelime.sequences.pipes.mapping import MappingConditionIndexRange

        seq = self.input.create_reader().map_if(
            stage=stage,
            condition=MappingConditionIndexRange(
                start=self.start,
                stop=self.stop,
                step=self.step,
            ),
        )

        self.grabber.grab_all(
            self.output.append_writer(seq),
            grab_context_manager=self.output.serialization_cm(),
            keep_order=False,
            parent_cmd=self,
            track_message=message,
        )


class AddRemoteCommand(RemoteCommandBase, title="remote-add"):
    """Upload samples to one or more remotes.

    Slicing and key options filter the samples to upload,
    but the whole dataset is always written out.
    """

    def run(self):
        from pipelime.stages import StageUploadToRemote

        self._run_remote_op(
            StageUploadToRemote(
                remotes=[r.get_url() for r in self.remotes],  # type: ignore
                keys_to_upload=self.keys,
            ),
            "Uploading data",
        )


class RemoveRemoteCommand(RemoteCommandBase, title="remote-remove"):
    """Remove one or more remote from a dataset.

    Slicing and key options filter the samples,
    but the whole dataset is always written out.

    NB: data is not removed from the remote data lake.
    """

    def run(self):
        from pipelime.stages import StageForgetSource

        remotes = [r.get_url() for r in self.remotes]  # type: ignore
        remove_all = remotes if not self.keys else []
        remove_by_key = {k: remotes for k in self.keys}

        self._run_remote_op(
            StageForgetSource(*remove_all, **remove_by_key), "Removing remote sources"
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

        def _flatten_dict(self, dict_, parent_key="", sep=".", prefix="+"):
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

        def _flatten_list(self, list_, parent_key, sep=".", prefix="+"):
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
            "Max number of samples to consider when creating the schema. "
            "Negative values count from the end, while if 0 all samples are checked."
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
            k: pl_types.ItemValidationModel(
                class_path=info.item_type,
                is_optional=(info.count_ != len(seq)),
                is_shared=info.is_shared,
            )
            for k, info in item_info.items_info.items()
        }

        sample_validation = pl_types.SampleValidationInterface(
            sample_schema=sample_schema,
            ignore_extra_keys=False,
            lazy=(self.max_samples == 0),
            max_samples=self.max_samples,
        ).dict(by_alias=True)

        if self.root_key_path:  # pragma: no branch
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
        seq = seq.map(self.stage)
        self.grabber.grab_all(
            self.output.append_writer(seq),
            grab_context_manager=self.output.serialization_cm(),
            keep_order=False,
            parent_cmd=self,
            track_message=f"Mapping data ({len(seq)} samples)",
        )


class SortCommand(PipelimeCommand, title="sort"):
    """Sort a dataset by metadata values or according to a custom sorting function."""

    sort_key: t.Optional[str] = pyd.Field(
        None,
        alias="k",
        description=(
            "A pydash-like key path. The path is built by splitting the mapping "
            "keys by `.` and enclosing list indexes within `[]`. "
            "Use `\\` to escape the `.` character."
        ),
    )
    sort_fn: t.Optional[pl_types.CallableDef] = pyd.Field(
        None,
        alias="f",
        description=(
            "A class path to a callable `(Sample) -> Any` to be used as key-function. "
            "Use `functools.cmp_to_key` to convert a compare function, "
            "ie, accepting two arguments, to a key function."
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

    def _sort_key_fn(self, x):
        return x.deep_get(self.sort_key)

    def run(self):
        if (self.sort_key is None) == (self.sort_fn is None):
            raise ValueError("You should define either `sort_key` or `sort_fn`")

        sort_fn = (
            self._sort_key_fn
            if self.sort_key is not None
            else self.sort_fn.value  # type: ignore
        )

        seq = self.input.create_reader()
        seq = seq.sort(sort_fn, lazy=False)
        self.grabber.grab_all(
            self.output.append_writer(seq),
            grab_context_manager=self.output.serialization_cm(),
            keep_order=False,
            parent_cmd=self,
            track_message=f"Sorting data ({len(seq)} samples)",
        )


class FilterCommand(PipelimeCommand, title="filter"):
    """Filter samples by metadata values or according to a custom sorting function."""

    filter_query: t.Optional[str] = pyd.Field(
        None,
        alias="q",
        description=("A dictquery (cfr. https://github.com/cyberlis/dictquery)."),
    )
    filter_fn: t.Optional[pl_types.CallableDef] = pyd.Field(
        None,
        alias="f",
        description=(
            "A `class.path.to.func` (or `file.py:func`) to "
            "a callable `(Sample) -> bool` returning True for any valid sample."
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

    @pyd.validator("filter_fn", always=True)
    def _check_filters(
        cls, v: t.Optional[pl_types.CallableDef], values: t.Mapping[str, t.Any]
    ) -> t.Optional[pl_types.CallableDef]:
        fquery = values.get("filter_query", None)
        if (v is None) == (fquery is None):
            raise ValueError("You should define either `filter_query` or `filter_fn`")
        return v

    def _filter_key_fn(self, x):
        return x.match(self.filter_query)

    def run(self):
        from pipelime.sequences import DataStream

        filter_fn = (
            self._filter_key_fn if self.filter_fn is None else self.filter_fn.value
        )

        # multi-processing friendly filtering
        class _WriterHelper:
            def __init__(self, output_pipe):
                self.stream = DataStream(output_pipe=output_pipe)
                self.curr_idx = 0

            def __call__(self, sample):
                if bool(sample):
                    self.stream.set_output(self.curr_idx, sample)
                    self.curr_idx += 1

        seq = self.input.create_reader()

        if self.output.zfill is None:
            self.output.zfill = seq.best_zfill()
        writer_helper = _WriterHelper(output_pipe=self.output.as_pipe())

        seq = seq.filter(filter_fn, lazy=True, insert_empty_samples=True)
        self.grabber.grab_all(
            seq,
            keep_order=True,
            parent_cmd=self,
            sample_fn=writer_helper,
            track_message=f"Filtering data ({len(seq)} samples)",
        )


class SliceCommand(PipelimeCommand, title="slice"):
    """Extract a subset of samples from a dataset."""

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

    slice: pl_interfaces.ExtendedInterval = pl_interfaces.ExtendedInterval.pyd_field(
        alias="s"
    )

    shuffle: t.Union[bool, pyd.PositiveInt] = pyd.Field(
        False,
        alias="shf",
        description=(
            "Shuffle the dataset before slicing. Optionally specify the random seed."
        ),
    )

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )

    def run(self):
        seq = self.input.create_reader()
        if self.shuffle:
            seq = seq.shuffle(
                seed=self.shuffle if not isinstance(self.shuffle, bool) else None
            )
        seq = seq.slice(
            start=self.slice.start, stop=self.slice.stop, step=self.slice.step
        )
        seq = self.output.append_writer(seq)
        self.grabber.grab_all(
            seq,
            grab_context_manager=self.output.serialization_cm(),
            keep_order=False,
            parent_cmd=self,
            track_message=f"Slicing dataset ({len(seq)} samples)",
        )


class SetMetadataCommand(FilterCommand, title="set-meta"):
    """Set a metadata for some or all samples in a dataset.

    The metadata is set only on samples selected by a dictquery or a filter function.
    """

    filter_fn: t.Optional[pl_types.CallableDef] = pyd.Field(
        None,
        alias="f",
        description=(
            "A `class.path.to.func` (or `file.py:func`) to "
            "a callable returning True for any valid sample.\n"
            "Accepted signatures:\n"
            "  `() -> bool`\n"
            "  `(index: int) -> bool`\n"
            "  `(index: int, sample: Sample) -> bool`\n"
            "  `(index: int, sample: Sample, source: SamplesSequence) -> bool`."
        ),
    )

    key_path: str = pyd.Field(
        ..., alias="k", description="The metadata key in pydash dot notation."
    )
    value: pl_types.YamlInput = pyd.Field(
        None, alias="v", description="The value to set, ie, any valid yaml/json value."
    )

    def _filter_key_fn(self, idx, x):
        return x.match(self.filter_query)

    def run(self) -> None:
        from pipelime.stages import StageSetMetadata

        seq = self.input.create_reader()
        seq = seq.map_if(
            stage=StageSetMetadata(key_path=self.key_path, value=self.value),
            condition=(
                self._filter_key_fn if self.filter_fn is None else self.filter_fn.value
            ),
        )
        seq = self.output.append_writer(seq)
        self.grabber.grab_all(
            seq,
            grab_context_manager=self.output.serialization_cm(),
            keep_order=False,
            parent_cmd=self,
            track_message=f"Setting metadata ({len(seq)} samples)",
        )


class FilterDuplicatesCommand(PipelimeCommand, title="filter-duplicates"):
    """Filter duplicated samples based on the hash of a set of items."""

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

    algorithm: str = pyd.Field(
        "sha256",
        description=(
            "The hashing algorithm from `hashlib` to use. Only algorithms that"
            "do not require parameters are supported."
        ),
    )

    keys: t.Union[str, t.Sequence[str]] = pyd.Field(
        ...,
        description=(
            "The keys to use for comparison. All items selected must be equal to"
            "consider two samples as duplicates."
        ),
    )

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )

    def run(self):
        from pipelime.stages import StageSampleHash

        seq = self.input.create_reader()
        hash_key = self._get_hash_key(list(seq[0].keys()))
        keys = [self.keys] if isinstance(self.keys, str) else self.keys
        stage = StageSampleHash(algorithm=self.algorithm, keys=keys, hash_key=hash_key)
        seq = seq.map(stage)

        # multi-processing friendly filtering
        class _WriterHelper:
            def __init__(self, output_pipe):
                self.stream = pls.DataStream(output_pipe=output_pipe)
                self.curr_idx = 0
                self.unique_hashes = set()

            def __call__(self, sample):
                sample_hash = sample[hash_key]()
                sample.remove_keys(hash_key)
                if sample_hash not in self.unique_hashes:
                    self.unique_hashes.add(sample_hash)
                    self.stream.set_output(self.curr_idx, sample)
                    self.curr_idx += 1

        if self.output.zfill is None:
            self.output.zfill = seq.best_zfill()
        writer_helper = _WriterHelper(output_pipe=self.output.as_pipe())

        # filter out samples that have a hash that appears more than once
        self.grabber.grab_all(
            seq,
            keep_order=True,
            parent_cmd=self,
            sample_fn=writer_helper,
            track_message=f"Filtering ({len(seq)} samples)",
        )

    def _get_hash_key(self, keys: t.Sequence[str]) -> str:
        """Get a key that has no conflict with the given keys.

        Args:
            keys (t.Sequence[str]): the keys of the samples

        Returns:
            str: the hash key
        """
        key = "hash"
        while True:
            if key not in keys:
                return key
            key += "_"
