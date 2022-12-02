# Stages

Let's focus some more on pipelime stages. [As you may already know](../get_started/operations.md#stages), stages are a special kind of operation that transforms individual samples of a sequence. When a stage is applied on a sequence, all of its samples are transformed independently.

If possible, you should always implement your operations as a stage, for the following reasons:

- They minimize the code you have to write.
- You get parallelization for free.
- Your operation will become a reusable node for many different pipelines.
- You will be able to manually run your operation from a command line interface.

## Entities And Actions

[Previously](../sequences/sequences.md), we have seen how to get and modify samples from a dataset. Now, consider again [the operations we implemented](../sequences/sequences.md#modifying-data):
1. Keeping only the samples with even index
2. Inverting the color of the images
3. Adding a new item called `color` with the average image color
4. Deleting the `maskinv` item

Points 2, 3 and 4 can be implemented as **stages**, while point 1 requires to remove samples from a sequence, thus violating one of the conditions to be a stage, i.e., input and outputs should have the same length.

Though you can implement your stage as a class derived from `pipelime.stages.SampleStage`, for most applications we recommend to use a more powerful mechanism based on auto-validated *entities*.
You need to define three components:
- An **input entity** defining the items that your stage expects to find in the input sample
- An **output entity** defining the items that your stage will put into the output sample
- An **action callable** accepting the input entity and returning the output entity

The input/output entities are classes derived from `pipelime.stages.BaseEntity`, which in turn is a [pydantic models](https://pydantic-docs.helpmanual.io/).

```{hint}
Pydantic models are dataclasses on steroids, they provide automatic de/serialization, validation, constructor, property-like fields generation and tons of interesting features aimed at reducing the amount of boilerplate code for plain python classes.
```

The entity model fields define the expected item keys and their types.
Considering the first operation *"Inverting the color of the images"*, they can be implemented as follows:

```python
from pipelime.stages import BaseEntity
import pipelime.items as pli

class InvertInput(BaseEntity):
    image: pli.ImageItem

class InvertOutput(BaseEntity):
    image: pli.ImageItem
```

Then, the action can just be a function that takes an `InvertInput` and returns an `InvertOutput`:

```python
def invert_action(x: InvertInput) -> InvertOutput:
    return InvertOutput.merge_with(x, image=x.image.make_new(255 - x.image()))
```

Note how the unrelated items are forwarded to the output using `merge_with`.
Also, we use `make_new` to replace the image with an item of the same type.

The benefit of this approach are multiple:
- The input and output entities are automatically validated, raising informative errors if the input sample is missing some items or if the items have the wrong type
- By defining full pydantic `Fields`, you can specify default values, constraints, help strings, etc.
- You can skip much of the boilerplate code and focus on the actual logic of your operation

To apply this action on a sequence, you have to wrap it inside a `StageEntity` and call the `map` method:

```python
from pipelime.stages import StageEntity

new_seq = seq.map(StageEntity(invert_action))
```

Similarly, when [running the `map` command from a configuration file](../cli/overview.md):

```yaml
map:
    input: ...
    output: ...
    stage:
        entity: class.path.to.invert_action
```

Where `class.path.to.invert_action` can be a `path/to/module.py:invert_action` as well.
As you can see you don't have to specify the input and output entities: they are automatically inferred from the action signature.

The next operation compute the average color value and adds a new item to the sample:

```python
from pipelime.stages import BaseEntity
import pipelime.items as pli
import numpy as np

class AverageColorInput(BaseEntity):
    image: pli.ImageItem

class AverageColorOutput(BaseEntity):
    color: pli.NumpyItem

def avg_color_action(x: AverageColorInput) -> AverageColorOutput:
    avg_color = np.mean(x.image(), axis=(0, 1))
    avg_color_item = pli.NpyNumpyItem(avg_color)
    return AverageColorOutput.merge_with(x, color=avg_color_item)
```

Finally, the last operation deletes the `maskinv` item:

```python
class RemoveMaskinvInput(BaseEntity):
    maskinv: pli.ImageItem

class RemoveMaskinvOutput(BaseEntity):
    pass

def remove_maskinv_action(x: RemoveMaskinvInput) -> RemoveMaskinvOutput:
    x_dict = x.dict()
    del x_dict["maskinv"]
    return RemoveMaskinvOutput(**x_dict)
```

Here we used the `dict` method of the underlying pydantic model to get a dictionary
representation of the input entity, we deleted the `maskinv` item, then we created
a new output entity from the remaining data.

## Parametrized Actions

In the examples above we have seen how to use any free function as an action.
However, if you need to keep some internal state, you can define a class with the `__call__` method instead.

For instance, let's say we want to saturate the image value to a given threshold:

```python
class SaturateInputOutput(BaseEntity):
    image: pli.ImageItem

class SaturateAction:
    def __init__(self, threshold: int):
        self._threshold = threshold

    def __call__(self, x: SaturateInputOutput) -> SaturateInputOutput:
        image = x.image().copy()  # make a copy to avoid modifying the original image
        image[image > self._threshold] = self._threshold
        return SaturateInputOutput.merge_with(x, image=x.image.make_new(image))
```

Then we can use it as follows:

```python
from pipelime.stages import StageEntity

new_seq = seq.map(StageEntity(SaturateAction(111)))
```

## Advanced Entity Features

Leveraging the power of pydantic models, you can define more complex entities that fit your needs.
First, `BaseEntity` forwards any item key that is not listed as field.
Though, you can drop them or even raise an error:

```python
# removes from the sample all the items except "image"
class DropExtraEntity(BaseEntity, extra="ignore"):
    image: pli.ImageItem

# raise an error if the sample contains items other than "image"
class StrictEntity(BaseEntity, extra="forbid"):
    image: pli.ImageItem
```

Also, you can add custom validation logic to your entities:

```python
class ColorImageEntity(BaseEntity):
    image: pli.ImageItem

    @validator("image")
    def check_color(cls, v):
        if v().shape[2] != 3:
            raise ValueError("The image must be a color image")
        return v
```

You can even perform advanced transformations on the input items, checkout [pydantic](https://pydantic-docs.helpmanual.io/)
for more details:

```python
class MaskedGrayImageEntity(BaseEntity):
    """A gray image with a mask.
    The grayscale image is computed from the RGB image if not provided.
    """

    image: pli.ImageItem = Field(None)
    grayscale: pli.ImageItem = Field(None)
    mask: pli.ImageItem

    @validator("image")
    def check_color_image(cls, v):
        if v is not None and v().shape[2] != 3:
            raise ValueError("The `image` must be a color image")
        return v

    @validator("grayscale", always=True)
    def check_gray_image(cls, v, values):
        if v is None:
            if "image" not in values or values["image"] is None:
                raise ValueError("Either `image` or `grayscale` must be provided")
            v = values["image"].make_new(values["image"]().mean(axis=2))
        return v

    @validator("mask")
    def check_mask(cls, v, values):
        if v().shape[2] != 1:
            raise ValueError("The `mask` must be single channel")
        if v().shape[:2] != values["grayscale"]().shape[:2]:
            raise ValueError("The `mask` and the `grayscale` image must have the same size")
        return v
```

Finally, you can let the user change the input entity type among a set of possible subclasses
of your annotated entity, or even more freely if you do not annotate your action callable.
Indeed, the concrete type of the input entity to use can be specified as:

```python
from pipelime.stages import StageEntity

new_seq = seq.map(StageEntity(action=your_action, input_type=YourInputEntity))

# or, equivalently
new_seq = seq.map(StageEntity(action="class.path.to.your_action", input_type="class.path.to.YourInputEntity"))
```

where `YourInputEntity` must be a subclass of the annotated input entity or, at least,
a subclass of `BaseEntity`.

## Full-fledged Stages

Though the previous approach is very powerful, it is not always the best choice.
For example, the key names are hardcoded into the entity models, so the user is forced to use the same names
or, at least, to insert a `remap-key` in his pipeline. A possible solution to support dynamic key names could be:

```python
from pydantic import BaseModel, Field, PrivateAttr, create_model

class SaturateAction(BaseModel):
    threshold: int = Field(..., description="The saturation threshold")
    image_key: str = Field("image", description="The key of the image item")

    def __call__(self, x: BaseEntity) -> SaturateInputOutput:
        image = getattr(x, self.image_key)().copy()
        image[image > self.threshold] = self.threshold
        return SaturateInputOutput.merge_with(x, image=getattr(x, self.image_key).make_new(image))
```

However, in this way we have lost the automatic validation of sample inputs.
Therefore, we could just derive from `SampleStage` and implement a brand-new `InvertStage` class:

```python
from pydantic import Field

from pipelime.sequences import Sample
from pipelime.stages import SampleStage

class InvertStage(SampleStage, title="invert"):
    """Inverts the colors of an image."""

    key: str = Field("image", description="The key of the image to invert.")

    def __call__(self, x: Sample) -> Sample:
        return x.set_value(self.key, 255 - x[self.key]())  # type: ignore
```

Though the items are not automatically parsed and validated, now we can easily support dynamic key names
as well as easily inject the stage into a pipeline, both programmatically:

```python
stage = InvertStage(key=...)
new_seq = seq.map(stage)
```

or from a configuration file using the `title` field given in the class definition:

```yaml
map:
    input: ...
    output: ...
    stage:
        invert:
            key: ...
```

All stages must implement the `__call__` method, accepting and returning a single sample.
The call method here simply reads the image item, inverts the colors and returns the new sample.
The dynamic key name is given through the `key` field, that defaults to the string "image", and includes a `description`. Though not essential, you should always set fields' descriptions because they are automatically used by pipelime to display a help message in the [CLI](../cli/overview.md).
Likewise, the class docstring is extracted and used as a description of the stage.

Using a stage to implement the average color computation is now pretty easy:

```python
from pydantic import Field

from pipelime.sequences import Sample
from pipelime.stages import SampleStage

class AverageColor(SampleStage, title="avg_color"):
    """Averages the color of an image."""

    image_key: str = Field("image", description="The key of the image to average.")
    avg_key: str = Field("color", description="The key of the average color.")

    def __call__(self, x: Sample) -> Sample:
        return x.set_value(self.avg_key, np.mean(x[self.image_key](), axis=(0, 1)))  # type: ignore
```

Instead, the removal of the `maskinv` item is achieved with the built-in `StageKeysFilter`.

## Stage Composition

To sequentially apply all the three stages, you can just combine them with the `>>` and `<<` operators:

```python
from pipelime.stages import StageKeysFilter

new_seq = seq.map(InvertStage() >> AverageColor() >> StageKeysFilter(key_list=["maskinv"], negate=True))
```

or, if you prefer, flippling the shift operator:

```python
from pipelime.stages import StageKeysFilter

new_seq = seq.map(StageKeysFilter(key_list=["maskinv"], negate=True) << AverageColor() << InvertStage())
```

Indeed, the left/right shift operators are just shorcuts for the `StageCompose` stage:

```python
from pipelime.stages import StageKeysFilter

new_seq = seq.map(StageCompose([InvertStage(), AverageColor(), StageKeysFilter(key_list=["maskinv"], negate=True)]))
```

```{tip}
When using built-in stages, you can use their titles as well:

`new_seq = seq.map({"compose": {"stages": [InvertStage(), AverageColor(), {"filter-keys": {"key_list": ["maskinv"], "negate": True}}]}})`
```
