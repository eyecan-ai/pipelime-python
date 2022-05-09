from pathlib import Path

from pydantic import Field

from pipelime.cli.model import PipelimeCommand
from pipelime.piper.model import PiperModel


class MyUselessCommand(PipelimeCommand):
    input_folder: Path = Field(..., piper_input=True)
    output_folder: Path = Field(..., piper_output=True)
    n: int = 10
    piper: PiperModel = PiperModel()

    def run(self) -> None:
        import time

        print(self.input_folder, self.output_folder)
        for x in self.track(range(self.n), message="Holy Pinoly"):
            time.sleep(0.1)


a = MyUselessCommand(input_folder=Path("/tmp/input"), output_folder=Path("/tmp/output"))
a.piper.token = "TOKEN"
a()
