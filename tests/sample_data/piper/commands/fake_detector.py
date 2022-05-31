import time
from typing import List

import numpy as np
from pydantic import Field

from pipelime.commands.interfaces import InputDatasetInterface, OutputDatasetInterface
from pipelime.items import YamlMetadataItem
from pipelime.piper import PipelimeCommand, PiperPortType
from pipelime.sequences import SamplesSequence


class FakeDetector(PipelimeCommand, title="fake_detector"):

    inputs: List[InputDatasetInterface] = Field(..., piper_port=PiperPortType.INPUT)
    outputs: List[OutputDatasetInterface] = Field(..., piper_port=PiperPortType.OUTPUT)
    fake_time: float = Field(0.0, description="Fake time.")

    def run(self) -> None:
        if len(self.inputs) != len(self.outputs):
            raise RuntimeError("Number of inputs and outputs must be the same.")

        for input_, output in zip(self.inputs, self.outputs):
            # Read input sequence
            seq = input_.create_reader()

            # Add metadata
            out_seq = SamplesSequence.from_list([])  # type: ignore
            for sample in self.track(seq):
                data = {"keypoints": [np.random.randint(0, 100, (10, 4)).tolist()]}
                sample["fake_detection"] = YamlMetadataItem(data)
                out_seq.append(sample)
                time.sleep(self.fake_time)

            # Write output sequence
            out_seq = output.append_writer(out_seq)
            [... for x in self.track(seq)]
