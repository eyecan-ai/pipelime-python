from pipelime.piper.progress.estimator.base import Estimator
from pipelime.piper.progress.estimator.naive import NaiveEstimator


class EstimatorFactory:
    """Factory for `Estimator`s"""

    DEFAULT_ESTIMATOR_TYPE = "NAIVE"

    CLASS_MAP = {
        "NAIVE": NaiveEstimator,
    }

    @classmethod
    def get_estimator(cls, type_: str = DEFAULT_ESTIMATOR_TYPE, **kwargs) -> Estimator:
        return cls.CLASS_MAP[type_](**kwargs)
