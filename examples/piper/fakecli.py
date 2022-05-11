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
        for x in self.track(range(self.n), message="Doing stuff..."):
            time.sleep(0.5)

        for x in self.track(range(self.n), message="Doing stuff 2..."):
            time.sleep(0.5)


if __name__ == "__main__":
    a = MyUselessCommand(
        input_folder=Path("/tmp/input"), output_folder=Path("/tmp/output")
    )
    a.piper.token = "TOKEN"
    a.piper.node = "UselessCommand_A"
    a()
    a = MyUselessCommand(
        input_folder=Path("/tmp/input"), output_folder=Path("/tmp/output")
    )
    a.piper.token = "TOKEN"
    a.piper.node = "UselessCommand_B"
    a()
    a = MyUselessCommand(
        input_folder=Path("/tmp/input"), output_folder=Path("/tmp/output")
    )
    a.piper.token = "TOKEN"
    a.piper.node = "UselessCommand_C"
    a()
