import typing as t
from enum import Enum
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
        description="The pipeline to run or a path to a YAML/JSON file "
        "(use <filepath>:<key-path> to load the definitions from a pydash-like path).\n"
        "The pipeline is defined as a mapping or a sequence of mappings where "
        "each key is a sequence operator to run, while the value gathers "
        "the arguments, ie, a single value, a sequence of values or a mapping.\n"
        "You can inspect the available operators by running `pipelime list --seq` and "
        "`pipelime list --seq --details`.",
    )
    input: pl_interfaces.InputDatasetInterface = pyd.Field(
        ..., description="Input dataset.", piper_port=PiperPortType.INPUT
    )
    output: pl_interfaces.OutputDatasetInterface = pyd.Field(
        ..., description="Output dataset.", piper_port=PiperPortType.OUTPUT
    )
    grabber: pl_interfaces.GrabberInterface = pyd.Field(
        default_factory=pl_interfaces.GrabberInterface,  # type: ignore
        description="Grabber options.",
    )

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
        from pipelime.sequences import build_pipe

        seq = self.input.create_reader()
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

    input: pl_interfaces.InputDatasetInterface = pyd.Field(
        ..., description="Input dataset.", piper_port=PiperPortType.INPUT
    )
    output: pl_interfaces.OutputDatasetInterface = pyd.Field(
        ..., description="Output dataset.", piper_port=PiperPortType.OUTPUT
    )
    grabber: pl_interfaces.GrabberInterface = pyd.Field(
        default_factory=pl_interfaces.GrabberInterface,  # type: ignore
        description="Grabber options.",
    )

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


class AddRemoteCommand(PipelimeCommand, title="remote-add"):
    """Upload samples to one or more remotes."""

    input: pl_interfaces.InputDatasetInterface = pyd.Field(
        ..., description="Input dataset.", piper_port=PiperPortType.INPUT
    )
    remotes: t.Union[
        pl_interfaces.RemoteInterface, t.Sequence[pl_interfaces.RemoteInterface]
    ] = pyd.Field(..., description="Remote data lakes addresses.")
    keys: t.Union[str, t.Sequence[str]] = pyd.Field(
        default_factory=list,
        description="Keys to upload. Leave empty to upload all the keys.",
    )
    output: t.Optional[pl_interfaces.OutputDatasetInterface] = pyd.Field(
        None,
        description="Optional output dataset with remote items.",
        piper_port=PiperPortType.OUTPUT,
    )
    grabber: pl_interfaces.GrabberInterface = pyd.Field(
        default_factory=pl_interfaces.GrabberInterface,  # type: ignore
        description="Grabber options.",
    )

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

    input: pl_interfaces.InputDatasetInterface = pyd.Field(
        ..., description="Input dataset.", piper_port=PiperPortType.INPUT
    )
    remotes: t.Union[
        pl_interfaces.RemoteInterface, t.Sequence[pl_interfaces.RemoteInterface]
    ] = pyd.Field(..., description="Remote data lakes addresses.")
    keys: t.Union[str, t.Sequence[str]] = pyd.Field(
        default_factory=list,
        description=(
            "Remove remotes on these keys only. Leave empty to affect all the keys."
        ),
    )
    output: pl_interfaces.OutputDatasetInterface = pyd.Field(
        None,
        description="Output dataset.",
        piper_port=PiperPortType.OUTPUT,
    )
    grabber: pl_interfaces.GrabberInterface = pyd.Field(
        default_factory=pl_interfaces.GrabberInterface,  # type: ignore
        description="Grabber options.",
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
        seq = self.output.append_writer(seq)
        with self.output.serialization_cm():
            self.grabber.grab_all(
                seq,
                keep_order=False,
                parent_cmd=self,
                track_message=f"Removing remotes ({len(seq)} samples)",
            )


class ValidateCommand(PipelimeCommand, title="validate"):
    """Print a minimal schema which will validate the given input. Also, if a schema
    is set on the input dataset, it will be used to validate the input."""

    input: pl_interfaces.InputDatasetInterface = pyd.Field(
        ..., description="Input dataset.", piper_port=PiperPortType.INPUT
    )
    max_samples: int = pyd.Field(
        0,
        description=(
            "Max number of samples to consider when creating the schema.  "
            "Set to 0 to check all the samples."
        ),
    )
    root_key_path: str = pyd.Field(
        "", description="Root key path for the output schema."
    )
    grabber: pl_interfaces.GrabberInterface = pyd.Field(
        default_factory=pl_interfaces.GrabberInterface,  # type: ignore
        description="Grabber options.",
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
            k: pl_interfaces.ItemValidationModel(
                class_path=info.class_path,
                is_optional=(info.count_ != len(seq)),
                is_shared=info.is_shared,
            ).dict(by_alias=True)
            for k, info in item_info.items_info().items()
        }
        self._print_schema(
            pl_interfaces.SampleValidationInterface(
                sample_schema=sample_schema,
                ignore_extra_keys=False,
                lazy=(self.max_samples == 0),
                max_samples=self.max_samples,
            )
        )

    def _print_schema(self, sample_schema):
        import json

        import pydash as py_

        through_json = json.loads(sample_schema.json(by_alias=True))
        if self.root_key_path:
            tmp_dict = {}
            py_.set_(tmp_dict, self.root_key_path, through_json)
            through_json = tmp_dict
        print("\n\nYAML/JSON schema definition:\n")
        print("****************************")
        print(json.dumps(through_json, indent=2))
        print("****************************")
        print("\n\nCommand line usage:\n")
        print("*********************")
        print(" ".join(self._flatten_dict(through_json)))
        print("*********************")

    def _flatten_dict(self, dict_, parent_key="", sep=".", prefix="--"):
        cmd_line = []
        for k, v in dict_.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else f"{prefix}{k}"
            if isinstance(v, t.Mapping):
                cmd_line.extend(
                    self._flatten_dict(v, parent_key=new_key, sep=sep, prefix=prefix)
                )
            elif isinstance(v, t.Sequence) and not isinstance(v, (str, bytes)):
                cmd_line.extend(
                    self._flatten_list(v, parent_key=new_key, sep=sep, prefix=prefix)
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
                    self._flatten_dict(v, parent_key=new_key, sep=sep, prefix=prefix)
                )
            elif isinstance(v, t.Sequence) and not isinstance(v, (str, bytes)):
                cmd_line.extend(
                    self._flatten_list(v, parent_key=new_key, sep=sep, prefix=prefix)
                )
            else:
                cmd_line.append(new_key)
                cmd_line.append(str(v))
        return cmd_line
