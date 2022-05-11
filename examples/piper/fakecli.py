from pathlib import Path

from pydantic import Field

from pipelime.piper.model import PipelimeCommand


class MyUselessCommand(PipelimeCommand):
    input_folder: Path = Field(..., piper_input=True)
    output_folder: Path = Field(..., piper_output=True)
    n: int = 10

    def run(self) -> None:
        import time

        print(self.input_folder, self.output_folder)
        for x in self.track(range(self.n), message="Holy Pinoly"):
            time.sleep(0.1)


if __name__ == "__main__":
    a = MyUselessCommand(
        input_folder=Path("/tmp/input"), output_folder=Path("/tmp/output")
    )
    a._piper.token = "TOKEN"
    a()
