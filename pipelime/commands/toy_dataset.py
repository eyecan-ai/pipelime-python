from pydantic import Field

import pipelime.commands.interfaces as pl_interfaces
from pipelime.piper import PipelimeCommand, PiperPortType


class ToyDatasetCommand(PipelimeCommand, title="toy_dataset"):
    """A generator of random data (images, masks, objects...)."""

    toy: pl_interfaces.ToyDatasetInterface = Field(
        ..., alias="t", description="Toy dataset creation options."
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
        seq = self.toy.create_dataset_generator()
        seq = self.output.append_writer(seq)
        with self.output.serialization_cm():
            self.grabber.grab_all(
                seq,
                keep_order=False,
                parent_cmd=self,
                track_message=f"Writing toy dataset ({len(seq)} samples)",
            )
