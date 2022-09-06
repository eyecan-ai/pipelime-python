from __future__ import annotations
import multiprocessing
import pydantic as pyd
import typing as t
from enum import Enum, auto

import pipelime.sequences as pls


class ReturnType(Enum):
    NO_RETURN = auto()
    SAMPLE = auto()
    SAMPLE_AND_INDEX = auto()


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
        self,
        sequence: pls.SamplesSequence,
        return_type: ReturnType = ReturnType.SAMPLE,
        size: t.Optional[int] = None,
        *,
        worker_init_fn: t.Union[
            t.Callable, t.Tuple[t.Callable, t.Sequence], None
        ] = None,
    ) -> _GrabContext:
        return _GrabContext(
            self,
            sequence,
            return_type=return_type,
            size=size,
            worker_init_fn=worker_init_fn,
        )


class _GrabWorker:
    def __init__(self, sequence: pls.SamplesSequence):
        self._sequence = sequence

    def _worker_fn_no_return(self, idx) -> None:
        _ = self._sequence[idx]  # pragma: no cover

    def _worker_fn_sample(self, idx) -> pls.Sample:
        return self._sequence[idx]  # pragma: no cover

    def _worker_fn_sample_and_index(self, idx) -> t.Tuple[int, pls.Sample]:
        return idx, self._sequence[idx]


class _GrabContext:
    def __init__(
        self,
        grabber: Grabber,
        sequence: pls.SamplesSequence,
        return_type: ReturnType,
        size: t.Optional[int],
        worker_init_fn: t.Union[t.Callable, t.Tuple[t.Callable, t.Sequence], None],
    ):
        self._grabber = grabber
        self._sequence = sequence
        self._return_type = return_type
        self._size = size
        self._pool = None
        self._worker_init_fn = (
            worker_init_fn
            if isinstance(worker_init_fn, tuple)
            else (worker_init_fn, ())
        )

    def __enter__(self):
        if self._grabber.num_workers == 0:
            self._pool = None
            it = iter(self._sequence)
            if self._worker_init_fn[0] is not None:
                self._worker_init_fn[0](*self._worker_init_fn[1])
            if self._return_type == ReturnType.SAMPLE_AND_INDEX:
                return enumerate(it)
            return it

        self._pool = multiprocessing.Pool(
            self._grabber.num_workers if self._grabber.num_workers > 0 else None,
            initializer=self._worker_init_fn[0],
            initargs=self._worker_init_fn[1],
        )
        runner = self._pool.__enter__()

        worker = _GrabWorker(self._sequence)
        if self._return_type == ReturnType.NO_RETURN:
            fn = worker._worker_fn_no_return
        elif self._return_type == ReturnType.SAMPLE:
            fn = worker._worker_fn_sample
        else:
            fn = worker._worker_fn_sample_and_index

        if self._grabber.keep_order:
            return runner.imap(
                fn,
                range(len(self._sequence) if self._size is None else self._size),
                chunksize=self._grabber.prefetch,
            )
        return runner.imap_unordered(
            fn,
            range(len(self._sequence) if self._size is None else self._size),
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
    sample_fn: t.Union[
        t.Callable[[pls.Sample], None], t.Callable[[pls.Sample, int], None], None
    ] = None,
    size: t.Optional[int] = None,
    grab_context_manager: t.Optional[t.ContextManager] = None,
    worker_init_fn: t.Union[t.Callable, t.Tuple[t.Callable, t.Sequence], None] = None,
):
    from inspect import signature, Parameter
    import contextlib

    if track_fn is None:
        track_fn = lambda x: x  # noqa: E731
    if sample_fn is None:
        sample_fn = lambda x: None  # noqa: E731
        return_type = ReturnType.NO_RETURN
    else:
        prms = signature(sample_fn).parameters
        return_type = (
            ReturnType.SAMPLE_AND_INDEX
            if (
                len(prms) > 1
                or next(iter(prms.values())).kind == Parameter.VAR_POSITIONAL
            )
            else ReturnType.SAMPLE
        )
    if grab_context_manager is None:
        grab_context_manager = contextlib.nullcontext()

    with grab_context_manager:
        ctx = grabber(
            sequence,
            return_type=return_type,
            size=size,
            worker_init_fn=worker_init_fn,
        )
        if return_type == ReturnType.SAMPLE_AND_INDEX:
            with ctx as gseq:
                for idx, sample in track_fn(gseq):
                    sample_fn(sample, idx)  # type: ignore
        else:
            with ctx as gseq:
                for sample in track_fn(gseq):
                    sample_fn(sample)  # type: ignore
