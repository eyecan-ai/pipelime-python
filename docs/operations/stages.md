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
from pipelime.stages.entities import BaseEntity
import pipelime.items as pli

class InvertInput(BaseEntity):
    image: pli.ImageItem

class InvertOutput(BaseEntity):
    image: pli.ImageItem
```

Then, the action can just be a function that takes an `InvertInput` and returns an `InvertOutput`:

```python
def invert_action(x: InvertInput) -> InvertOutput:
    return InvertOutput.merge(x, image=(255 - x.image()))
```

Note how the unrelated items are forwarded to the output using `merge`.
Also, since we are overwriting the existing `image` key and we want to use the same item type,
we just pass the raw numpy array.

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

The next operation computes the average color value and adds a new item to the sample:

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
    return AverageColorOutput.merge(x, color=avg_color)
```

The value assigned to `color` is a raw numpy array, which is silently converted to the type stated in `AverageColorOutput`. In general, the actual concrete type is chosen according to the following rules:
1. if the input entity has an item with the same key and a compatible (covariant) type,
such type is used
2. if the output entity is a concrete class, its type is used
3. if the output entity is an abstract class, its default type is used

In the above example, since we expect not `color` item in the input, the default type for `NumpyItem`, ie, `NpyNumpyItem`, will be used.

Finally, the last operation deletes the `maskinv` item:

```python
class RemoveMaskinvInput(BaseEntity):
    maskinv: pli.ImageItem

def remove_maskinv_action(x: RemoveMaskinvInput) -> BaseEntity:
    out_dict = {k: v for k, v in x.dict().items() if k != "maskinv"}
    return BaseEntity(**out_dict)
```

Here we used the `dict` method of the underlying pydantic model to get a dictionary
representation of the input entity.

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
        return SaturateInputOutput.merge(x, image=image)
```

Then we can use it as follows:

```python
from pipelime.stages import StageEntity

new_seq = seq.map(StageEntity(SaturateAction(111)))
```

## Optional Items

Sometimes you may want to define an entity that can contain some optional items.
For instance, we may expect a label in the input, but if not found, we just set it to 0:

```python
class OptionalLabelInput(BaseEntity):
    image: pli.ImageItem
    label: pli.NumpyItem = pli.TxtNumpyItem(0)
```

Also, if you want to exclude some items from the output when a condition is met,
just set it as optional and use the special `None` value:

```python
from typing import Optional

class DebuggableOutput(BaseEntity):
    image: pli.ImageItem
    debug: Optional[pli.ImageItem]

class DebuggableAction:
    def __init__(self, debug: bool):
        self._debug = debug

    def __call__(self, x):
        # do some cool processing
        # ...
        if self._debug:
            debug_image = ...
        else:
            debug_image = None
        return DebuggableOutput.merge(x, debug=debug_image)
```

## Entity Value Parsing

Besides high-level validation, item values can also be parsed and converted to a more convenient type. The most common use case is to parse a JSON/YAML metadata file into a pydantic model:

```python
from typing import Sequence
from pydantic import BaseModel

from pipelime.stages.entities import ParsedItem

class MetadataModel(BaseModel):
    keypoints: t.Sequence[t.Tuple[float, float]]
    label: int
    description: str

class EInput(BaseEntity):
    image: pli.ImageItem
    metadata: ParsedItem[pli.MetadataItem, MetadataModel]
```

The `metadata` has been defined as a `ParsedItem` class with two type parameters:
1. the first one is the type of the item to parse
2. the second one is the type of the parsed value

So, when the `EInput` entity is created from a sample, the `metadata` item type must be
covariant with `pli.MetadataItem` and a `MetadataModel` instance is created from the raw
metadata value. Then, calling `metadata()` will return the parsed value instead the raw
item value.

`ParsedItem` can be used in output entities as well and can be built from a pipelime item,
its raw value or the parsed value. In any case, both the item and the parsed value must be
_constructable_ from the a raw item value, so a full validation is always performed.
Also, if you don't care about the actual item type, you can just use
`ParsedData[ParsedType]` instead of `ParsedItem[ItemType, ParsedType]`.

`ParsedItem` and `ParsedData` are fully compatible with the `Optional` and `DynamicKey`
(see below) features.

## Dynamic Key Names

To make your actions more flexible and reusable, the actual item key names they work on can be defined at runtime. For instance, let's say each input sample is composed of two images
`image_1` and `image_2` and we'd want to apply the [`SaturateAction`](#parametrized-actions) on both.
Instead of expecting an item called `image`, we can define a private field with a special
`DynamicKey` value:

```python
from pipelime.stages.entities import DynamicKey

class SaturateDynamicInput(BaseEntity):
    _image = DynamicKey(pli.ImageItem)
```

Note that the `_image` field will not considered when parsing and validating an input sample.

Then, the new action expects the actual image key name as a parameter and explicitly calls a validation on it:

```python
class SaturateDynamicAction:
    def __init__(self, threshold: int, image_key: str = "image"):
        self._threshold = threshold
        self._image_key = image_key

    def __call__(self, x: SaturateDynamicInput):
        image = x._image.validate(self._image_key)

        image = image().copy()  # make a copy to avoid modifying the original image
        image[image > self._threshold] = self._threshold
        return BaseEntity.merge(x, **{self._image_key: image})
```

The `validate` method checks that the given key is present in the input sample
with the required type, then returns the corresponding item. Note how the kwargs notation
has been used to overwrite the actual `_image` field with the new value.

`DynamicKey` is fully compatible with the usual field definitions and other features:
* the full signature is `DynamicKey(item_type, default_value, default_factory, **field_kwargs)`,
where `field_kwargs` is any other kwarg accepted by `pydantic.Field`
* `DynamicKey(ParsedItem[ItemType, ParsedType])` is also supported

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

You can even perform advanced transformations on the input items,
checkout [pydantic](https://pydantic-docs.helpmanual.io/) for more details:

```python
class MaskedGrayImageEntity(BaseEntity):
    """A gray image with a mask.
    The grayscale image is computed from the RGB image if not provided.
    """

    image: pli.ImageItem = None
    grayscale: pli.ImageItem = None
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

Though the previous approach is very powerful, sometimes it might not fit all your needs
and you want to write a full-fledged stage class. The `invert_action` can be converted
to a stage class as follows:

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

Though the items are not automatically parsed and validated, we can easily support dynamic key names
as well as inject the stage into a pipeline, both programmatically:

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

Using a stage to implement the average color computation is pretty easy as well:

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

`new_seq = seq.map({"compose": ["invert", "avg_color", {"filter-keys": {"key_list": ["maskinv"], "negate": True}}]}})`
```
