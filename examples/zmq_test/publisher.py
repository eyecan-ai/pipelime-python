import time
from pathlib import Path

from typer import Option, Typer

from pipelime.piper.piper import Piper
from pipelime.piper.progress import Tracker, ZmqTrackCallback

app = Typer()


@app.command()
@Piper.command(inputs=["input_folder"], outputs=["output_folder"])
def fake_cli(
    input_folder: Path = Option(...),
    output_folder: Path = Option(...),
    n: int = Option(...),
) -> None:
    print(input_folder, output_folder)
    tracker = Tracker(ZmqTrackCallback())
    for x in tracker.track(range(n), "Holy Pinoly"):
        print(x)
        time.sleep(0.1)


if __name__ == "__main__":
    app()
