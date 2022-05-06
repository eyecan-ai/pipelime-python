from pathlib import Path

from pydantic import Field

from pipelime.cli.model import CliModel
from pipelime.piper.model import PiperModel


class MyUselessCommand(CliModel):
    input_folder: Path
    output_folder: Path
    n: int = 10

    piper = Field(PiperModel(inputs=["input_folder"], outputs=["output_folder"]))

    def run(self) -> None:
        import time

        print(self.input_folder, self.output_folder)
        for x in self.track(range(self.n), message="Holy Pinoly"):
            time.sleep(0.1)


a = MyUselessCommand(input_folder=Path("/tmp/input"), output_folder=Path("/tmp/output"))
# a.piper.token = "TOKEN"
a.piper.node = "NODE"
a()
