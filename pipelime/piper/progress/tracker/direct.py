from pipelime.piper.progress.model import ProgressUpdate
from pipelime.piper.progress.tracker.base import TrackCallback
from pipelime.piper.progress.listener.base import ListenerCallback


class DirectTrackCallback(TrackCallback):
    """Tracker callback that forwards the progress to some listener callbacks."""

    def __init__(self, *callbacks: ListenerCallback) -> None:
        from weakref import finalize

        super().__init__()
        self._callbacks = callbacks
        self._finalizer = finalize(self, self.stop_callbacks)

        for cb in self._callbacks:
            cb.on_start()

    def update(self, prog: ProgressUpdate):
        for cb in self._callbacks:
            cb.on_update(prog)

    def stop_callbacks(self):
        for cb in self._callbacks:
            cb.on_stop()
