from abc import ABC, abstractmethod
from pipelime.sequences import Sample


# TODO: Spook support
class SampleStage(ABC):
    @abstractmethod
    def __call__(self, x: Sample) -> Sample:
        pass


class StageIdentity(SampleStage):
    def __call__(self, x: Sample) -> Sample:
        return x


class StageCompose(SampleStage):
    def __init__(self, *stages: SampleStage):
        """Applies a sequence of stage."""
        super().__init__()
        self._stages = stages

    def __call__(self, x: Sample) -> Sample:
        for s in self._stages:
            x = s(x)
        return x
