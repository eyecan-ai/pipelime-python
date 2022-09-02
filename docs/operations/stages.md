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

Consider the example from the previous tutorial:
```{admonition} Quote
:class: tip

Let's modify the "mini mnist" dataset by:
1. Keeping only the samples with even index.
2. Inverting the color of the images.
3. Adding a new item called "color" with the average image color.
4. Deleting the "maskinv" item.
```

Some of these operations (2, 3 and 4) can be implemented as stages. Point 1 requires to remove samples from a sequence, violating one of the conditions to be a stage (input and outputs should have the same length).

We will detail the implementation of the point 2.

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

The sequence returned by `map` is another sequence on which the invert stage is applied. As many other operators, `map` transforms the data **lazily**: the stage is only executed when accessing individual samples.

Pipelime provides a special stage called `StageCompose` that is a sequential composition of other stages, useful to apply many stages at once.

Suppose we also want to reimplement points 3 and 4 from the previous tutorial, this is the code for point 3 (adding an item with the image average color):

```python 
class AverageColor(plst.SampleStage, title="avg_color"):
    """Average the color of an image."""

    image_key: str = Field("image", description="The key of the image to average.")
    avg_key: str = Field("avg_color", description="The key of the average color.")

    def __call__(self, x: pls.Sample) -> pls.Sample:
        return x.set_value(self.avg_key, np.mean(x[self.image_key](), axis=(0, 1)))  # type: ignore
```

Point 4 (removing the "maskinv" item), can really just be done with pipelime built-in stages, so, no need to implement anything.

Supposing 
```python

all_in_one_stage = plst.StageCompose(
    Invert(),
    AverageColor(),
    plst.StageKeysFilter(key_list=["maskinv"], negate=True),
)
new_seq = seq.map(all_in_one_stage)
``` 

## Usage - CLI

TODO