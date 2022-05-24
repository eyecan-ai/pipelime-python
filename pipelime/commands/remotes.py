import typing as t

import pydantic as pyd

import pipelime.commands.interfaces as pl_interfaces
from pipelime.piper import PipelimeCommand, PiperPortType


class AddRemoteCommand(PipelimeCommand, title="remote-add"):
    """Upload samples to one or more remotes."""

    input: pl_interfaces.InputDatasetInterface = pyd.Field(
        ..., description="Input dataset.", piper_port=PiperPortType.INPUT
    )
    remotes: t.Sequence[pl_interfaces.RemoteInterface] = pyd.Field(
        ..., description="Remote data lakes addresses."
    )
    keys: t.Sequence[str] = pyd.Field(
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

    def run(self):
        from pipelime.stages import StageUploadToRemote

        seq = self.input.create_reader().map(
            StageUploadToRemote(
                remotes=[r.get_url() for r in self.remotes], keys_to_upload=self.keys
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
            track_message=f"Uploading data ({len(seq)} samples)...",
        )
