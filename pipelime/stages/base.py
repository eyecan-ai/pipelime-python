from abc import ABC, abstractmethod
from pipelime.sequences import Sample

import typing as t


class SampleStage(ABC):
    """Base class for all sample stages"""
    @abstractmethod
    def __call__(self, x: Sample) -> Sample:
        pass


class StageIdentity(SampleStage):
    """Returns the input sample"""
    def __call__(self, x: Sample) -> Sample:
        return x


class StageLambda(SampleStage):
    def __init__(self, func: t.Callable[[Sample], Sample]):
        """Apply a callable to the sample.

        :param func: the callable to apply
        :type func: t.Callable[[Sample], Sample]
        """
        self._func = func

    def __call__(self, x: Sample) -> Sample:
        return self._func(x)


class StageCompose(SampleStage):
    def __init__(self, *stages: SampleStage):
        """Applies a sequence of stages.

        :param stages: the stages to apply
        :type stages: SampleStage
        """
        super().__init__()
        self._stages = stages

    def __call__(self, x: Sample) -> Sample:
        for s in self._stages:
            x = s(x)
        return x
