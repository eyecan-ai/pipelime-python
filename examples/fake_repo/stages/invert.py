from pydantic import Field

import pipelime.sequences as pls
import pipelime.stages as plst


class Invert(plst.SampleStage, title="invert"):
    """Invert the colors of an image."""

    key: str = Field("image", description="The key of the image to invert.")

    def __call__(self, x: pls.Sample) -> pls.Sample:
        return x.set_value(self.key, 255 - x[self.key]())  # type: ignore


class AverageColor(plst.SampleStage, title="avg_color"):
    """Average the color of an image."""

    image_key: str = Field("image", description="The key of the image to average.")
    avg_key: str = Field("avg_color", description="The key of the average color.")

    def __call__(self, x: pls.Sample) -> pls.Sample:
        return x.set_value(self.avg_key, np.mean(x[self.image_key](), axis=(0, 1)))  # type: ignore
