import typing as t

import pydantic as pyd

import pipelime.piper as piper
from pipelime.commands.interfaces import (
    GrabberInterface,
    InputDatasetInterface,
    OutputDatasetInterface,
)
from pipelime.commands.piper import PiperDAG, piper_dag


class StopIt(piper.PipelimeCommand, title="stopit"):
    input: InputDatasetInterface = InputDatasetInterface.pyd_field(
        description="Fake input", piper_port=piper.PiperPortType.INPUT
    )
    output: OutputDatasetInterface = OutputDatasetInterface.pyd_field(
        description="Fake output", piper_port=piper.PiperPortType.OUTPUT
    )
    stop: bool = True

    @classmethod
    def init_from_checkpoint(cls, checkpoint: piper.CheckpointNamespace, /, **data):
        """Derived classes may override to support command resuming."""
        ckpt_stop = checkpoint.read_data("stop", None)
        if ckpt_stop is not None:
            data["stop"] = not ckpt_stop
        return super().init_from_checkpoint(checkpoint, **data)

    def run(self):
        self.command_checkpoint.write_data("stop", self.stop)
        if self.stop:
            raise ValueError("Stop requested")

        seq = self.output.append_writer(self.input.create_reader())
        GrabberInterface().grab_all(  # type: ignore
            seq, parent_cmd=self, track_message="Don't stop me now!"
        )


@piper_dag
class CatAndSplit(PiperDAG, title="cat-and-split"):
    """Cat and splits datasets when it's raining outside."""

    do_shuffle: bool = pyd.Field(..., description="Whether to shuffle the data")
    slices: t.Union[pyd.PositiveInt, t.Sequence[pyd.PositiveInt]] = pyd.Field(
        ..., description="The final slice size - only the last value is used"
    )

    main_data: InputDatasetInterface = InputDatasetInterface.pyd_field(
        piper_port=piper.PiperPortType.INPUT
    )
    datalist: t.Sequence[InputDatasetInterface] = pyd.Field(
        ..., description="A list of datasets", piper_port=piper.PiperPortType.INPUT
    )

    output: OutputDatasetInterface = OutputDatasetInterface.pyd_field(
        description="The output root folder", piper_port=piper.PiperPortType.OUTPUT
    )

    def create_graph(self, folder_debug):
        import pipelime.commands as plcmd

        input_cat = plcmd.ConcatCommand.lazy()(
            inputs=self.datalist, output=folder_debug / "cat_data"  # type: ignore
        )
        add_main = plcmd.ConcatCommand.lazy()(
            inputs=[self.main_data, input_cat.output],  # type: ignore
            output=folder_debug / "all_cat",  # type: ignore
        )

        nodes: t.List = [input_cat, add_main]
        nodes.append(
            StopIt.lazy()(
                input=nodes[-1].output,
                output=folder_debug / "stop_out",  # type: ignore
            )
        )

        slice_size = (
            self.slices[-1] if isinstance(self.slices, t.Sequence) else self.slices
        )
        nodes.append(
            plcmd.SliceCommand.lazy()(  # type: ignore
                input=nodes[-1].output,
                output=self.output,
                slice={"stop": slice_size},
                shuffle=self.do_shuffle,  # type: ignore
            )
        )

        return nodes
