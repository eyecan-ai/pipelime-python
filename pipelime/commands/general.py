import typing as t
from pathlib import Path

import pydantic as pyd

import pipelime.commands.interfaces as pl_interfaces
from pipelime.piper import PipelimeCommand, PiperPortType


class PipeCommand(PipelimeCommand, title="pipe"):
    """A general-purpose command to build up linear pipelines."""

    operations: t.Union[
        str, t.Mapping[str, t.Any], t.Sequence[t.Union[str, t.Mapping[str, t.Any]]]
    ] = pyd.Field(
        ...,
        alias="op",
        description="The pipeline to run or a path to a YAML/JSON file "
        "(use <filepath>:<key-path> to load the definitions from a pydash-like path).\n"
        "The pipeline is defined as a mapping or a sequence of mappings where "
        "each key is a sequence operator to run, while the value gathers "
        "the arguments, ie, a single value, a sequence of values or a mapping.",
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
    _input_validator = pl_interfaces.InputDatasetInterface.pyd_validator("input")

    output: pl_interfaces.OutputDatasetInterface = (
        pl_interfaces.OutputDatasetInterface.pyd_field(
            alias="o", piper_port=PiperPortType.OUTPUT
        )
    )
    _output_validator = pl_interfaces.OutputDatasetInterface.pyd_validator("output")

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )
    _grabber_validator = pl_interfaces.GrabberInterface.pyd_validator("grabber")

    _pipe_list: t.Union[
        str, t.Mapping[str, t.Any], t.Sequence[t.Union[str, t.Mapping[str, t.Any]]]
    ] = pyd.PrivateAttr()

    def __init__(self, **data):
        import pydash as py_
        import yaml

        super().__init__(**data)

        if isinstance(self.operations, str):
            filepath, _, root_key = (
                self.operations.rpartition(":")
                if ":" in self.operations
                else (self.operations, None, None)
            )
            filepath = Path(filepath)
            if filepath.exists():
                with filepath.open() as f:
                    self._pipe_list = yaml.safe_load(f)
                    if root_key is not None:
                        self._pipe_list = py_.get(  # type: ignore
                            self._pipe_list, root_key, default=None
                        )
            else:
                self._pipe_list = yaml.safe_load(str(self.operations))
        else:
            self._pipe_list = self.operations

        if not self._pipe_list:
            raise ValueError(f"Invalid pipeline: {self.operations}")

    def run(self):
        from pipelime.sequences import build_pipe, SamplesSequence

        seq = self.input.create_reader() if self.input is not None else SamplesSequence
        seq = build_pipe(self._pipe_list, seq)
        seq = self.output.append_writer(seq)

        with self.output.serialization_cm():
            self.grabber.grab_all(
                seq,
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
    _input_validator = pl_interfaces.InputDatasetInterface.pyd_validator("input")

    output: pl_interfaces.OutputDatasetInterface = (
        pl_interfaces.OutputDatasetInterface.pyd_field(
            alias="o", piper_port=PiperPortType.OUTPUT
        )
    )
    _output_validator = pl_interfaces.OutputDatasetInterface.pyd_validator("output")

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )
    _grabber_validator = pl_interfaces.GrabberInterface.pyd_validator("grabber")

    def run(self):
        seq = self.input.create_reader()
        seq = self.output.append_writer(seq)
        with self.output.serialization_cm():
            self.grabber.grab_all(
                seq,
                keep_order=False,
                parent_cmd=self,
                track_message=f"Cloning data ({len(seq)} samples)",
            )


class ConcatCommand(PipelimeCommand, title="cat"):
    """Concatenate two or more datasets."""

    inputs: t.Sequence[
        pl_interfaces.InputDatasetInterface
    ] = pl_interfaces.InputDatasetInterface.pyd_field(
        alias="i", piper_port=PiperPortType.INPUT
    )
    _inputs_validator = pl_interfaces.InputDatasetInterface.pyd_validator("inputs")

    output: pl_interfaces.OutputDatasetInterface = (
        pl_interfaces.OutputDatasetInterface.pyd_field(
            alias="o", piper_port=PiperPortType.OUTPUT
        )
    )
    _output_validator = pl_interfaces.OutputDatasetInterface.pyd_validator("output")

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )
    _grabber_validator = pl_interfaces.GrabberInterface.pyd_validator("grabber")

    @pyd.validator("inputs")
    def check_inputs(cls, v):
        if len(v) < 2:
            raise ValueError("You need at least two inputs.")
        return v

    def run(self):
        input_it = iter(self.inputs)
        seq = next(input_it).create_reader()
        for input_ in input_it:
            seq = seq.cat(input_.create_reader())
        seq = self.output.append_writer(seq)
        with self.output.serialization_cm():
            self.grabber.grab_all(
                seq,
                keep_order=False,
                parent_cmd=self,
                track_message=f"Writing data ({len(seq)} samples)",
            )


class AddRemoteCommand(PipelimeCommand, title="remote-add"):
    """Upload samples to one or more remotes."""

    input: pl_interfaces.InputDatasetInterface = (
        pl_interfaces.InputDatasetInterface.pyd_field(
            alias="i", piper_port=PiperPortType.INPUT
        )
    )
    _input_validator = pl_interfaces.InputDatasetInterface.pyd_validator("input")

    remotes: t.Union[
        pl_interfaces.RemoteInterface, t.Sequence[pl_interfaces.RemoteInterface]
    ] = pl_interfaces.RemoteInterface.pyd_field(alias="r")
    _remotes_validator = pl_interfaces.RemoteInterface.pyd_validator("remotes")

    keys: t.Union[str, t.Sequence[str]] = pyd.Field(
        default_factory=list,
        alias="k",
        description="Keys to upload. Leave empty to upload all the keys.",
    )

    output: t.Optional[
        pl_interfaces.OutputDatasetInterface
    ] = pl_interfaces.OutputDatasetInterface.pyd_field(
        alias="o",
        is_required=False,
        description="Optional output dataset with remote items.",
        piper_port=PiperPortType.OUTPUT,
    )
    _output_validator = pl_interfaces.OutputDatasetInterface.pyd_validator("output")

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )
    _grabber_validator = pl_interfaces.GrabberInterface.pyd_validator("grabber")

    @pyd.validator("remotes", "keys")
    def validate_remotes(cls, v):
        return (
            v if not isinstance(v, (str, bytes)) and isinstance(v, t.Sequence) else [v]
        )

    def run(self):
        from pipelime.stages import StageUploadToRemote

        seq = self.input.create_reader().map(
            StageUploadToRemote(
                remotes=[r.get_url() for r in self.remotes],  # type: ignore
                keys_to_upload=self.keys,
            )
        )

        if self.output is not None:
            seq = self.output.append_writer(seq)
            with self.output.serialization_cm():
                self._grab_all(seq)
        else:
            self._grab_all(seq)

    def _grab_all(self, seq):
        self.grabber.grab_all(
            seq,
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
    _input_validator = pl_interfaces.InputDatasetInterface.pyd_validator("input")

    remotes: t.Union[
        pl_interfaces.RemoteInterface, t.Sequence[pl_interfaces.RemoteInterface]
    ] = pl_interfaces.RemoteInterface.pyd_field(alias="r")
    _remotes_validator = pl_interfaces.RemoteInterface.pyd_validator("remotes")

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
    _output_validator = pl_interfaces.OutputDatasetInterface.pyd_validator("output")

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )
    _grabber_validator = pl_interfaces.GrabberInterface.pyd_validator("grabber")

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
        seq = self.output.append_writer(seq)
        with self.output.serialization_cm():
            self.grabber.grab_all(
                seq,
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
            import json

            return json.dumps(self.schema_def, indent=2)

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
    _input_validator = pl_interfaces.InputDatasetInterface.pyd_validator("input")

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
    _grabber_validator = pl_interfaces.GrabberInterface.pyd_validator("grabber")

    output_schema_def: t.Optional[OutputSchemaDefinition] = pyd.Field(
        None,
        description="YAML/JSON schema definition",
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
                class_path=info.class_path,
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
        )
        through_json = json.loads(sample_validation.json(by_alias=True))
        if self.root_key_path:
            import pydash as py_

            tmp_dict = {}
            py_.set_(tmp_dict, self.root_key_path, through_json)
            through_json = tmp_dict

        self.output_schema_def = ValidateCommand.OutputSchemaDefinition(
            schema_def=through_json
        )
        self.output_cmd_line_schema = ValidateCommand.OutputCmdLineSchema(
            schema_def=through_json
        )
