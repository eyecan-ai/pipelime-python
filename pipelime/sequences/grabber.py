from __future__ import annotations

import multiprocessing.context as mp_context
import multiprocessing.pool as mp_pool
import typing as t
from enum import Enum, auto

import pydantic.v1 as pyd

import pipelime.sequences as pls


class ReturnType(Enum):
    NO_RETURN = auto()
    SAMPLE = auto()
    SAMPLE_AND_INDEX = auto()


class Grabber(pyd.BaseModel, extra="forbid", copy_on_model_validation="none"):
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
    allow_nested_mp: bool = pyd.Field(
        False,
        description=(
            "Whether to allow nested multiprocessing. If True, the workers "
            "will be spawned as non-daemon processes (which may lead to zombie "
            "processes and not released resources)."
        ),
    )

    def __call__(
        self,
        sequence: pls.SamplesSequence,
        return_type: ReturnType = ReturnType.SAMPLE,
        size: t.Optional[int] = None,
        *,
        worker_init_fn: t.Optional[t.Tuple[t.Callable, t.Sequence]] = None,
    ) -> _GrabContext:
        return _GrabContext(
            self,
            sequence,
            return_type=return_type,
            size=size,
            worker_init_fn=worker_init_fn,
            allow_nested_mp=self.allow_nested_mp,
        )


class _GrabWorker:
    def __init__(self, sequence: pls.SamplesSequence):
        self._sequence = sequence

    def _worker_fn_no_return(self, idx) -> None:
        _ = self._sequence[idx]

    def _worker_fn_sample(self, idx) -> pls.Sample:
        return self._sequence[idx]

    def _worker_fn_sample_and_index(self, idx) -> t.Tuple[int, pls.Sample]:
        return idx, self._sequence[idx]


class _NoDaemonSpawnProcess(mp_context.SpawnProcess):
    @property
    def daemon(self):
        return False

    @daemon.setter
    def daemon(self, value):
        pass


class _NoDaemonSpawnContext(mp_context.SpawnContext):
    Process = _NoDaemonSpawnProcess


class _GrabContext:
    def __init__(
        self,
        grabber: Grabber,
        sequence: pls.SamplesSequence,
        return_type: ReturnType,
        size: t.Optional[int],
        worker_init_fn: t.Optional[t.Tuple[t.Callable, t.Sequence]],
        allow_nested_mp: bool = False,
    ):
        self._grabber = grabber
        self._sequence = sequence
        self._return_type = return_type
        self._size = size
        self._pool = None
        self._worker_init_fn = (None, ()) if worker_init_fn is None else worker_init_fn
        self._allow_nested_mp = allow_nested_mp

    @staticmethod
    def wrk_init(extra_modules, session_temp_dir, user_init_fn):
        from pipelime.choixe.utils.io import PipelimeTmp
        from pipelime.cli.utils import PipelimeSymbolsHelper

        PipelimeTmp.SESSION_TMP_DIR = session_temp_dir

        PipelimeSymbolsHelper.set_extra_modules(extra_modules)
        PipelimeSymbolsHelper.import_everything()

        if user_init_fn[0] is not None:
            user_init_fn[0](*user_init_fn[1])

    def __enter__(self):
        from pipelime.choixe.utils.io import PipelimeTmp
        from pipelime.cli.utils import PipelimeSymbolsHelper

        if self._grabber.num_workers == 0:
            # SINGLE PROCESS
            self._pool = None
            it = iter(self._sequence)
            if self._worker_init_fn[0] is not None:
                self._worker_init_fn[0](*self._worker_init_fn[1])
            if self._return_type == ReturnType.SAMPLE_AND_INDEX:
                return enumerate(it)
            return it

        # MULTIPLE PROCESSES

        if self._allow_nested_mp:
            # Spawn processes as non-daemon processes, to allow nested multiprocessing.
            # This is because (from the Python documentation):
            # "...a daemonic process is not allowed to create child processes. Otherwise
            # a daemonic process would leave its children orphaned if it gets terminated
            # when its parent process exits."
            # Thus, this option should be used with caution, as it may lead to zombie
            # processes and not released resources.
            context_cls = _NoDaemonSpawnContext
        else:
            context_cls = mp_context.SpawnContext

        self._pool = mp_pool.Pool(
            self._grabber.num_workers if self._grabber.num_workers > 0 else None,
            initializer=_GrabContext.wrk_init,
            initargs=(
                PipelimeSymbolsHelper.extra_modules,
                PipelimeTmp.SESSION_TMP_DIR,
                self._worker_init_fn,
            ),
            context=context_cls(),
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
    import contextlib
    from inspect import Parameter, signature

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

    if isinstance(worker_init_fn, t.Sequence):
        if len(worker_init_fn) == 1:
            worker_init_fn = (worker_init_fn[0], ())
        elif len(worker_init_fn) != 2:
            raise ValueError(
                "The worker_init_fn argument must be a callable or a tuple of "
                "a callable and its arguments."
            )
        worker_init_fn = tuple(worker_init_fn)
    elif worker_init_fn is not None:
        worker_init_fn = (worker_init_fn, ())

    with grab_context_manager:
        ctx = grabber(
            sequence,
            return_type=return_type,
            size=size,
            worker_init_fn=worker_init_fn,
        )
        if return_type == ReturnType.SAMPLE_AND_INDEX:
            with ctx as gseq:
                for idx, sample in track_fn(gseq):  # type: ignore
                    sample_fn(sample, idx)  # type: ignore
        else:
            with ctx as gseq:
                for sample in track_fn(gseq):  # type: ignore
                    sample_fn(sample)  # type: ignore
