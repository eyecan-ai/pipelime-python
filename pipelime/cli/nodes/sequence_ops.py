import pydantic as pyd
from pathlib import Path

from pipelime.cli.nodes import PipelimeCommand, PiperNode


class GrabberPlugin(PipelimeCommand, title="grabber"):
    """Multiprocessing grabbing
    options."""

    num_workers: int = pyd.Field(0, description="Number of processes to spawn.")
    prefetch: int = pyd.Field(2, description="Number of samples to prefetch.")

    def grab_all(self, sequence, keep_order):
        from pipelime.sequences import Grabber, grab_all

        grabber = Grabber(
            num_workers=self.num_workers, prefetch=self.prefetch, keep_order=keep_order
        )
        grab_all(grabber, sequence)


class SplitCommand(PiperNode, title="split"):
    """Splits a sequence into several sequences."""

    input_folder: pyd.DirectoryPath = pyd.Field(..., description="The input folder.")
    output_folder: Path = pyd.Field(..., description="The output folder.")
    perc: pyd.confloat(gt=0, le=1) = pyd.Field(  # type:ignore
        ..., description="The percentage of the split."
    )
    grabber: GrabberPlugin = GrabberPlugin()  # type: ignore

    def run(self):
        from pipelime.sequences import SamplesSequence

        seq = SamplesSequence.from_underfolder(self.input_folder)  # type: ignore
        seq = seq.split(start=0, stop=self.perc * len(seq)).to_underfolder(
            self.output_folder
        )
        self.grabber.grab_all(seq, keep_order=False)
