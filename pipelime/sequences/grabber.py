from __future__ import annotations
import multiprocessing
import pydantic as pyd
import typing as t

import pipelime.sequences as pls


class Grabber(pyd.BaseModel, extra="forbid"):
    num_workers: int = pyd.Field(
        0,
        description=(
            "The number of processes to spawn. If negative, "
            "the number of (logical) cpu cores is used."
        ),
    )
    prefetch: pyd.PositiveInt = pyd.Field(
        2, description="The number of samples loaded in advanced by each worker."
    )
    keep_order: bool = pyd.Field(
        False, description="Whether to retrieve the samples in the original order."
    )

    def __call__(
        self, sequence: pls.SamplesSequence, no_return: bool = False
    ) -> _GrabContext:
        return _GrabContext(self, sequence, no_return)


class _GrabWorker:
    def __init__(self, sequence: pls.SamplesSequence):
        self._sequence = sequence

    def _worker_fn(self, idx) -> pls.Sample:
        return self._sequence[idx]  # pragma: no cover

    def _noreturn_worker_fn(self, idx) -> None:
        _ = self._sequence[idx]  # pragma: no cover


class _GrabContext:
    def __init__(
        self, grabber: Grabber, sequence: pls.SamplesSequence, no_return: bool
    ):
        self._grabber = grabber
        self._sequence = sequence
        self._no_return = no_return
        self._pool = None

    def __enter__(self):
        if self._grabber.num_workers == 0:
            self._pool = None
            return iter(self._sequence)

        self._pool = multiprocessing.Pool(
            self._grabber.num_workers if self._grabber.num_workers > 0 else None
        )
        runner = self._pool.__enter__()

        worker = _GrabWorker(self._sequence)
        fn = worker._noreturn_worker_fn if self._no_return else worker._worker_fn

        if self._grabber.keep_order:
            return runner.imap(
                fn,
                range(len(self._sequence)),
                chunksize=self._grabber.prefetch,
            )
        return runner.imap_unordered(
            fn,
            range(len(self._sequence)),
            chunksize=self._grabber.prefetch,
        )

    def __exit__(self, exc_type, exc_value, traceback):
        if self._pool is not None:
            self._pool.__exit__(exc_type, exc_value, traceback)  # type: ignore


def grab_all(
    grabber: Grabber,
    sequence: pls.SamplesSequence,
    *,
    track_fn: t.Optional[t.Callable[[t.Iterable], t.Iterable]] = None,
    sample_fn: t.Optional[t.Callable[[pls.Sample], None]] = None,
):
    no_return = sample_fn is None
    if track_fn is None:
        track_fn = lambda x: x  # noqa: E731
    if sample_fn is None:
        sample_fn = lambda x: None  # noqa: E731
    with grabber(sequence, no_return) as gseq:
        for sample in track_fn(gseq):
            sample_fn(sample)
