from pipelime.piper.progress.estimator.factory import EstimatorFactory
from pipelime.piper.progress.estimator.base import Estimator


class TestEstimatorFactory:
    def test_get_estimator(self):
        estimator = EstimatorFactory.get_estimator()
        assert isinstance(estimator, Estimator)
