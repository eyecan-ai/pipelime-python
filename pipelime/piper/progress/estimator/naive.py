import time

from pipelime.piper.progress.estimator.base import Estimator


class NaiveEstimator(Estimator):
    """A naive estimator that simply estimates the speed and ETA based on the current
    progress and the biased EWA time between steps"""

    def __init__(self, alpha: float = 0.9):
        super().__init__()
        self.alpha = alpha

    def reset(self, total: int) -> None:
        self._avg_dt = -1
        self._progress = 0
        self._total = total
        self._start_time = time.time()
        self._last = self._start_time

    def tick(self, advance: int = 1) -> None:
        now = time.time()
        if advance > 0:
            new_delta = (now - self._last) / advance
            if self._avg_dt < 0:
                self._avg_dt = new_delta
            else:
                self._avg_dt = self._avg_dt * self.alpha + new_delta * (1 - self.alpha)
        self._progress += advance
        self._last = now

    @property
    def start_time(self) -> float:
        return self._start_time

    @property
    def elapsed(self) -> float:
        return self._last - self._start_time

    @property
    def speed(self) -> float:
        return 1 / self._avg_dt if self._avg_dt != 0 else float("inf")

    @property
    def eta(self) -> float:
        if self._avg_dt < 0:
            return -1
        return self._avg_dt * (self._total - self._progress)
