from pathlib import Path

from pipelime.piper import Field, PipelimeCommand, PiperPortType


class MyUselessCommand(PipelimeCommand):
    input_folder: Path = Field(..., piper_port=PiperPortType.INPUT)
    output_folder: Path = Field(..., piper_port=PiperPortType.OUTPUT)
    n: int = 10

    def run(self) -> None:
        import time

        print(self.input_folder, self.output_folder)
        for x in self.track(range(self.n), message="Doing stuff..."):
            time.sleep(0.5)

        for x in self.track(range(self.n), message="Doing stuff 2..."):
            time.sleep(0.5)


class FakeSum(PipelimeCommand):
    input_folders: list[Path] = Field(..., piper_port=PiperPortType.INPUT)
    output_folder: Path = Field(..., piper_port=PiperPortType.OUTPUT)

    def run(self) -> None:
        import time

        print(self.input_folders, self.output_folder)
        for x in self.track(range(5), message="Summing stuff..."):
            time.sleep(0.5)


if __name__ == "__main__":
    a = MyUselessCommand(
        input_folder=Path("/tmp/input"), output_folder=Path("/tmp/output")
    )
    a.set_piper_info(token="TOKEN", node="UselessCommand_A")
    a()
    a = MyUselessCommand(
        input_folder=Path("/tmp/input"), output_folder=Path("/tmp/output")
    )
    a.set_piper_info(token="TOKEN", node="UselessCommand_B")
    a()
    a = MyUselessCommand(
        input_folder=Path("/tmp/input"), output_folder=Path("/tmp/output")
    )
    a.set_piper_info(token="TOKEN", node="UselessCommand_C")
    a()
