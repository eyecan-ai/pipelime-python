# Stages

Let's focus some more on pipelime stages. [As you may already know](../get_started/entities.md), stages are a special kind of operation that transforms individual samples of a sequence. When a stage is applied on a sequence, all of its samples are transformed independently.

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

Points 2, 3 and 4 can be implemented as **stages**, while point 1 requires to remove samples from a sequence, thus violating one of the conditions to be a stage, i.e., input and outputs should have the same length.

We will detail the implementation of point 2 "inverting the color of the images".

Stages are subclasses of `plst.SampleStage`, which in turn is a [pydantic](https://pydantic-docs.helpmanual.io/) model. If you are not familiar with pydantic, you should take a look at it

```{admonition} TL;DR
:class: note

Pydantic models are dataclasses on steroids, they provide automatic de/serialization, validation, constructor, property-like fields generation and tons of interesting features aimed at reducing the amount of boilerplate code for plain python classes.
```

This is the full code of the `InvertStage` class:

```python
from pydantic import Field

import pipelime.sequences as pls
import pipelime.stages as plst


class InvertStage(plst.SampleStage, title="invert"):
    """Inverts the colors of an image."""

    key: str = Field("image", description="The key of the image to invert.")

    def __call__(self, x: pls.Sample) -> pls.Sample:
        return x.set_value(self.key, 255 - x[self.key]())  # type: ignore
```

All stages must implement the `__call__` method, accepting and returning a single sample. The call method here simply reads the image item, inverts the colors and returns the new sample.
You may notice that the class has a `key` field, that defaults to the string "image", and includes a `description`. Though not essential, you should always set fields' descriptions because they are automatically used by pipelime to display a help message in the [CLI](../cli/cli.md).
Also, you may notice a `title` field in the class definition. Again, this is not mandatory, but it serves as a user-friendly alias to the full class name whenever you need to refer to that stage in a pipeline.

## Applying a Stage

To apply `InvertStage` to a sequence, you have to call the `map` method:

```python
stage = InvertStage()
new_seq = seq.map(stage)
```

The sequence returned by `map` is a **new** sequence on which the invert stage is applied. As many other operators, `map` transforms the data **lazily**: the stage is only executed when accessing individual samples.

Note that instead of explicitly creating a new stage, you can use its title, possibly with parameters passed as a dictionary:

```python
new_seq = seq.map("invert")
...
new_seq = seq.map({"invert": {"key": "image"}})
```

Implementing point 3 is now pretty easy:

```python
class AverageColor(plst.SampleStage, title="avg_color"):
    """Averages the color of an image."""

    image_key: str = Field("image", description="The key of the image to average.")
    avg_key: str = Field("avg_color", description="The key of the average color.")

    def __call__(self, x: pls.Sample) -> pls.Sample:
        return x.set_value(self.avg_key, np.mean(x[self.image_key](), axis=(0, 1)))  # type: ignore
```

While point 4 is achieved with the built-in `StageKeysFilter`.
To sequentially apply all three stages, you can just combine them with the `>>` and `<<` operators:

```python
new_seq = seq.map(InvertStage() >> AverageColor() >> plst.StageKeysFilter(key_list=["maskinv"], negate=True))

# or equivalently
new_seq = seq.map(plst.StageKeysFilter(key_list=["maskinv"], negate=True) << AverageColor() << InvertStage())
```

Indeed, the left/right shift operators are just shorcuts for the `StageCompose` stage:

```python
new_seq = seq.map(plst.StageCompose([InvertStage(), AverageColor(), plst.StageKeysFilter(key_list=["maskinv"], negate=True)]))

# or equivalently
new_seq = seq.map(plst.StageCompose(["invert", "avg_color", {"filter-keys": {"key_list": ["maskinv"], "negate": True}}]))
```
