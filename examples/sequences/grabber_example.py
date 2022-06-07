if __name__ == "__main__":
    from pathlib import Path
    from pipelime.sequences import Grabber, SamplesSequence, grab_all

    seq = SamplesSequence.from_underfolder(  # type: ignore
        Path(__file__).resolve().absolute().parents[2]
        / "tests/sample_data/datasets/underfolder_minimnist"
    )
    seq = seq.shuffle(seed=42)
    writer = seq.to_underfolder("./writer_output", exists_ok=True).enumerate()

    # here comes the magic
    grabber = Grabber(num_workers=4)  # type: ignore
    grab_all(
        grabber,
        writer,
        sample_fn=(
            lambda x: print("sample #", int(x["~idx"]()), sep="")  # type: ignore
        ),
    )
