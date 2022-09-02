# Stages

Let's focus some more on pipelime stages. As you may know from the "Basics" section of this tutorial, stages are a special kind of operation that transforms individual samples of a sequence. When a stage is applied on a sequence, all of its samples are transformed independently.

If possible, you should always implement your operations as a stage, for the following reasons:

- They minimize the code you have to write.
- You get parallelization for free.
- Your operation will become a reusable node for many different pipelines.
- You will be able to manually run your operation from a command line iterface.

## Necessary Modules

Firstly, you should import the following modules:

```python
import pipelime.sequences as pls
import pipelime.stages as plst
```

## Definition

Let's write a stage that inverts the colors of an image, like in the example from the previous tutorial. 

Stages are subclasses of `plst.SampleStage`, which in turn are pydantic models. If you are not familiar with pydantic, you should take a look at it. TL;DR: pydantic models are dataclasses on steroids, they provide automatic de/serialization, validation, constructor and property-like fields generation and tons of interesting features aimed at reducing the amount of boilerplate code for plain python classes.

This is the full code of the `Invert` stage:

```python
from pydantic import Field

import pipelime.sequences as pls
import pipelime.stages as plst


class Invert(plst.SampleStage, title="invert"):
    """Invert the colors of an image."""

    key: str = Field("image", description="The key of the image to invert.")

    def __call__(self, x: pls.Sample) -> pls.Sample:
        return x.set_value(self.key, 255 - x[self.key]())  # type: ignore
```

All stages must implement the `__call__` method, accepting and returning a single sample. The call method here simply reads the image item, inverts the colors and returns the new sample.

You may notice that the class has a `key` field, that defaults to the string "image". Fields descriptions are automatically used by pipelime to display a help message in the CLI, although not necessary, you should always add them. 

Another thing is that along the `SampleStage` superclass, there is a `title` parameter in the class definition. This is also not necessary, it serves as a user-friendly alias to the full class name in case you want to use this stage from the CLI.

## Usage - API

Suppose you want to apply the `Invert` stage to a sequence of your choice, then you simply need to create the stage object and call the `map` method:

```python
stage = Invert()
new_seq = seq.map(stage)
```

TODO

## Usage - CLI
