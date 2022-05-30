import time
from pipelime.piper.progress.estimator.base import Estimator
from pipelime.piper.progress.estimator.naive import NaiveEstimator


class TestNaiveEstimator:
    def test_estimator(self):
        N = 10
        estimator = NaiveEstimator(alpha=0.8)
        assert isinstance(estimator, Estimator)

        # Reset the estimator
        estimator.reset(total=N)

        # Check that the estimator is reset
        assert estimator.start_time > 0
        assert estimator.elapsed == 0
        assert estimator.speed < 0
        assert estimator.eta < 0

        for _ in range(N):
            # Tick the estimator
            time.sleep(0.1)
            estimator.tick(advance=1)

            # Check that the estimator is updated
            assert estimator.start_time > 0
            assert estimator.elapsed > 0
            assert estimator.speed > 0
            assert estimator.eta >= 0
