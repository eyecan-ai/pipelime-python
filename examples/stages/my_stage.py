from pipelime.stages import SampleStage
from pipelime.sequences import Sample


class MyStage(SampleStage):
    source_key: str
    target_key: str

    def __call__(self, x: Sample) -> Sample:
        from pipelime.items import NumpyItem

        if self.source_key in x:
            value = x[self.source_key]
            if isinstance(value, NumpyItem):
                x = x.set_value_as(
                    self.target_key,
                    self.source_key,
                    float(value()) * 2.5,  # type: ignore
                )
        return x
