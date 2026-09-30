from pydantic import Field

import pipelime.sequences as pls
import pipelime.stages as plst


class Invert(plst.SampleStage, title="invert"):
    """Invert the colors of an image."""

    key: str = Field("image", description="The key of the image to invert.")

    def __call__(self, x: pls.Sample) -> pls.Sample:
        return x.set_value(self.key, 255 - x[self.key]())  # type: ignore
