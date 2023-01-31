from typing import ContextManager


class ContextManagerList:
    def __init__(self, *args: ContextManager):
        self._cms = args

    def __enter__(self):
        for cm in self._cms:
            cm.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        for cm in self._cms:
            cm.__exit__(exc_type, exc_value, traceback)


class CatchSignals:
    def __init__(self, signals=None):
        import signal
        self._signals = signals if signals is not None else [signal.SIGINT, signal.SIGTERM]
        self._original_handlers = {}
        self._interrupted = False
        self._released = False

    @property
    def interrupted(self):
        return self._interrupted

    def __enter__(self):
        import signal
        self._original_handlers = {}
        self._interrupted = False
        self._released = False

        for sig in self._signals:
            self._original_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, self.handler)

        return self

    def handler(self, signum, frame):
        self.release()
        self._interrupted = True

    def __exit__(self, type, value, tb):
        self.release()

    def release(self):
        import signal
        if self._released:
            return False

        for sig in self._signals:
            signal.signal(sig, self._original_handlers[sig])

        self._released = True
        return True
