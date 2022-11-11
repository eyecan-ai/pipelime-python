# Schema Validation

When you deploy a pipeline in production, you want to make sure that the input data is valid.
Pipelime commands provide a standard way to define such schema both for input and output sequences. For example, this might be a configuration file for the `clone` command:

```yaml
input:
  folder: input/path
  schema:                           # ☚ The "schema" argument of the "clone" command
    sample_schema:                  # ☚ How a sample should look like
      image:                        # ☚ The "image" item key
        class_path: ImageItem       # ☚ The expected item type (can be a base class)
        is_optional: false          # ☚ Whether to raise an error if the key is missing
        is_shared: false            # ☚ Whether the item should be shared
      label:
        class_path: TxtNumpyItem
        is_optional: true
        is_shared: false
      camera:
        class_path: MetaDataItem
        is_optional: true
        is_shared: true
    ignore_extra_keys: false        # ☚ Whether to raise an error if the sample has extra keys
    lazy: false                     # ☚ Whether to check each sample when it is accessed or just once when building the sequence
    max_samples: 0                  # ☚ If "lazy" is False, at most "max_samples" are checked
output: output/path
```

You usually don't have to write such schema by hand. Instead, if you have a dataset fulfilling a target schema, just call `pipelime validate` and copy-paste the output, possibly tweaking some values, for example:

```bash
$ pipelime validate +i input_dataset
```

```bash
>>>
...
📦 output_schema_def: yaml schema definition
input:
  schema:
    sample_schema:
      image:
        class_path: PngImageItem
        is_optional: false
        is_shared: false
      label:
        class_path: TxtNumpyItem
        is_optional: true
        is_shared: false
      camera:
        class_path: YamlMetaDataItem
        is_optional: true
        is_shared: true
    ignore_extra_keys: false
    lazy: true
    max_samples: 0
...
```

## Adding A Custom Validation Function

Though this simple definition may be enough for most cases, more complex validation schemes can be easily developed if you don't mind writing a bit of code. For example, you may want to check that the `image` item has 3 channels, so you write a function that raises an exception if the validation fails and returns the parsed item otherwise:

```python
from pipelime.items import ImageItem

def check_image_channels(image_item: ImageItem) -> ImageItem:
    image = image_item()
    if image.shape[2] != 3:
        raise ValueError(f'Image has {image.shape[2]} channels, but 3 are expected')
    return image_item
```

Then, you can add it to the schema definition:

```yaml
input:
  folder: input/path
  schema:
    sample_schema:
      image:
        class_path: ImageItem
        is_optional: false
        is_shared: false
        validator: class.path.to.check_image_channels
...
```

Where the `class.path.to.check_image_channels` may be a `path/to/script.py:check_image_channels` as well. Note that the other checks, i.e., type check, whether is optional, wheter is shared, are still performed before calling your custom validator.

## Custom Sample Validation

If you feel comfortable with [pydantic](https://pydantic-docs.helpmanual.io/) you can even write a custom sample validator as a pydantic model. To validate a sample, pipelime tries to instantiate the model with the sample's items as input keywords, so the previous example can be rewritten as:

```python
from pydantic import BaseModel, Field, validator
import pipelime.items as pli

class CustomSampleSchema(BaseModel, extra="forbid"):
    image: pli.ImageItem
    label: pli.TxtNumpyItem = Field(default_factory=pli.TxtNumpyItem)
    camera: pli.MetaDataItem = Field(default_factory=pli.MetaDataItem)

    @pyd.validator("image")
    def validate_image(cls, image_item: pli.ImageItem) -> pli.ImageItem:
        if image_item.is_shared:
            raise ValueError('Image must not be shared.')
        image = image_item()
        if image.shape[2] != 3:
            raise ValueError(f'Image has {image.shape[2]} channels, but 3 are expected')
        return image_item

    @pyd.validator("label")
    def validate_label(cls, label_item: pli.TxtNumpyItem) -> pli.TxtNumpyItem:
        if label_item.is_shared:
            raise ValueError('Label must not be shared.')

    @pyd.validator("camera")
    def validate_camera(cls, camera_item: pli.MetaDataItem) -> pli.MetaDataItem:
        if not camera_item.is_shared:
            raise ValueError('Camera must be shared.')
```

Then, replace the `sample_schema` with the model's class path:

```yaml
input:
  folder: input/path
  schema:
    sample_schema: class.path.to.CustomSampleSchema
    ignore_extra_keys: false        # ☚ NB: This is ignored if you use a pydantic model
    lazy: false
    max_samples: 0
output: output/path
```

Of course, this approach becomes useful only when you want to perform complex checks, e.g.,
that the `image` item size is equal to a value declared inside the `camera` metadata item.

## Piped Validation

Validation can be performed as a step of a pipeline as well.
To this end, `SamplesSequence` provides the `validate_samples` method, which takes the `sample_schema` as input and raise an exception if the validation fails:

```python
from pydantic import BaseModel
from pipelime.sequences import SamplesSequence
from pipelime.items import ImageItem, NumpyItem
from pipelime.utils.pydantic_types import SampleValidationInterface

class MiniMNISTSampleValidator(BaseModel):
    image: ImageItem
    label: NumpyItem

seq = SamplesSequence.from_underfolder("datasets/mini_mnist")
seq = seq.validate_samples(
  sample_schema=SampleValidationInterface(
    sample_schema=MiniMNISTSampleValidator, lazy=False, max_samples=1
  )
)
```
