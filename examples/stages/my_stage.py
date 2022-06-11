from pipelime.stages import SampleStage
from pipelime.sequences import Sample


class MyStage(SampleStage):
    source_key: str
    target_key: str

    def __call__(self, x: Sample) -> Sample:
        from pipelime.items import NumpyItem, ImageItem

        if self.source_key in x:
            item = x[self.source_key]

            # we want numpy items, but not images, though they are ndarrays as well
            if isinstance(item, NumpyItem) and not isinstance(item, ImageItem):
                x = x.set_value_as(
                    self.target_key,
                    self.source_key,
                    float(item()) * 2.5,  # type: ignore
                )
        return x
