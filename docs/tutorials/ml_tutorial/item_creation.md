
# Adding New Items

In the previous tutorials we have seen how [to create a new dataset from scratch](./convert_to_underfolder.md) and how [to split it in three subsets](./dataset_splitting.md).
Each sample comes from the iris dataset and provides four features, namely, the length and width of the petals and the sepals. We want to train a network to classify the iris flowers according to their species using only the _area_ of the petals and sepals. Therefore, we will now build a simple pipeline with a custom stage to add such new features to each sample of the dataset.

## The Easy Way: Actions And Entities

A pipeline stage is a general operation that receives, processes and returns one sample at a time.
Though you can create a stage by directly subclassing the `SampleStage` class, it is often easier
and more convenient to use the Action/Entity framework, where:
- _Actions_ are plain functions or callable classes accepting and producing _Entities_
- _Entities_ are simplified Pydantic models to parse and validate samples

```{hint}
If you are not familiar with Pydantic, take a look at the [official documentation](https://docs.pydantic.dev/). Pydantic offers a new way to define, parse, validate and create data models in Python, somehow similar to dataclasses, but with a lot more features.
```

## Input And Output Entities

First, we need to define the required inputs and the expected outputs of our stage.
To compute the two areas, we need all the four features of the iris samples,
namely, `SepalLength`, `SepalWidth`, `PetalLength`, `PetalWidth`.
Therefore, we define an input entity with the same fields:

```python
from pipelime.stages.entities import BaseEntity
import pipelime.items as pli

class IrisInputEntity(BaseEntity):
    SepalLength: pli.NumpyItem
    SepalWidth: pli.NumpyItem
    PetalLength: pli.NumpyItem
    PetalWidth: pli.NumpyItem
```

Note how we have used the `NumpyItem` abstract type to accept any numpy-like data.

The output entity is even simpler, since we only need to declare two new fields:

```python
class IrisAreaOutputEntity(BaseEntity):
    SepalArea: pli.NumpyItem
    PetalArea: pli.NumpyItem
```

Again, the `NumpyItem` abstract type is used to include any numpy-like data.

Now every time `IrisInputEntity` is built from a sample, items are checked against the declared names and types. Unless you ask for a different behavior, extra items are silently added as attributes and non-pydantic types are allowed with basic checks.

## The Action Callable

The action is a function that receives an `IrisInputEntity` and returns an `IrisAreaOutputEntity`:

```python
@register_action(title="iris-areas")
def compute_areas(x: IrisInputEntity) -> IrisAreaOutputEntity:
    """Computes the areas of the petals and sepals."""
    sepal_area = float(x.SepalLength()) * float(x.SepalWidth())
    petal_area = float(x.PetalLength()) * float(x.PetalWidth())
    return IrisAreaOutputEntity.merge(x, SepalArea=sepal_area, PetalArea=petal_area)
```

A few important things to note:
* the `@register_action` decorator is not required, but allows you to just use the action's `title` in the pipeline configuration as well as to get the list of available actions when running `pipelime list` on your package or module
* the value of the fields of the input entity `x` are accessed by _calling_ the item, as usual.
* the `merge` method is called to forward the input entity `x` while adding the new fields
* no need to explicitly create the output item instances, as long as you provide a compatible _raw data_.

Though the parsing of the input sample into the `IrisInputEntity` ensures the existence of the required fields, no check is performed on their actual content. To this end, two mechanisms are available, depending on your needs. First, you might use standard `pydantic` validators:

```python
import numpy as np
from pydantic import validator
from pipelime.stages.entities import BaseEntity
import pipelime.items as pli

class IrisInputEntity(BaseEntity):
    SepalLength: pli.NumpyItem
    SepalWidth: pli.NumpyItem
    PetalLength: pli.NumpyItem
    PetalWidth: pli.NumpyItem

    @validator("*")
    def check_values(cls, value: pli.NumpyItem):
        raw = value()
        if raw is None or raw.size != 1:
            raise ValueError("All values must be scalars")
        if raw <= 0:
            raise ValueError("All values must be positive")
        return value
```

However, for more complex scenarios, you might want to define your own custom class
for validation and parsing, eg:

```python
from typing import Optional
import numpy as np
from pipelime.stages.entities import BaseEntity, ParsedItem
import pipelime.items as pli

# The "parsed value" class can be any class, including a Pydantic model
# Either way, it should be possible to make an instance from raw item data
# NB: we want float scalar inputs when creating the output entity (see below)
class IrisFeature:
    def __init__(self, raw_data: Optional[np.ndarray, float]):
        if raw_data is None:
            raise ValueError("Missing value")
        if isinstance(raw_data, np.ndarray):
            if raw_data.size != 1:
                raise ValueError("All values must be scalars")
            if raw_data <= 0:
                raise ValueError("All values must be positive")
            self._value = float(raw_data)
        else:
            self._value = raw_data

    @property
    def value(self) -> float:
        return self._value

    # a special method to get back the original raw data
    def __to_item_data__(self) -> np.ndarray:
        return np.array([self._value])


class IrisInputEntity(BaseEntity):
    SepalLength: ParsedItem[pli.NumpyItem, IrisFeature]
    SepalWidth: ParsedItem[pli.NumpyItem, IrisFeature]
    PetalLength: ParsedItem[pli.NumpyItem, IrisFeature]
    PetalWidth: ParsedItem[pli.NumpyItem, IrisFeature]

class IrisAreaOutputEntity(BaseEntity):
    SepalArea: ParsedItem[pli.NumpyItem, IrisFeature]
    PetalArea: ParsedItem[pli.NumpyItem, IrisFeature]
```

Entity field types are declared as `ParsedItem`, which wraps together the expected item type and a class responsible for parsing and validating the data. Now _calling_ the fields within the action returns an instance of the `IrisFeature` class:

```python
@register_action(title="iris-areas")
def compute_areas(x: IrisInputEntity) -> IrisAreaOutputEntity:
    """Computes the areas of the petals and sepals."""
    sepal_area = x.SepalLength().value * x.SepalWidth().value
    petal_area = x.PetalLength().value * x.PetalWidth().value
    return IrisAreaOutputEntity.merge(x, SepalArea=sepal_area, PetalArea=petal_area)
```

Note how the output entity is built now:
1. `IrisFeature` instances are created from the float scalars `sepal_area` and `petal_area`
1. The `__to_item_data__` method is called to get back the raw data
1. A `NpyNumpyItem`, which is the default for `NumpyItem`, is created from raw data

The `ParsedItem` class can be used in many other context and it is expecially suitable for metadata parsing to/from a pydantic model. See the [relevant documentation](../../operations/stages.md) for more details.

## Running The Action

An action can be run on a dataset by wrapping it in a `StageEntity`. Then, the `map` method of the `SamplesSequence` class applies the stage to each sample in the dataset:

```python
from pipelime.stages import StageEntity
from pipelime.sequences import SamplesSequence

# assuming IrisDataset class is defined in the same module
seq = SamplesSequence.iris().map(StageEntity(compute_areas))
print(seq[0]["SepalArea"](), seq[1]["PetalArea"]())
```

A corresponding pipeline can be defined in a YAML file with the `map` command:

```yaml
map:
    stage:
        entity: iris-areas
```

where we have used the `title` of the action, since it has been registered.
The pipeline from the previous step of this recipe can now be extended to include the new action:

```yaml
nodes:
    generate:
        pipe:
            operations: iris
            output: $tmp/iris_dataset
            grabber: $var(nproc, default=4)
    data_split:
        split:
            input: $tmp/iris_dataset
            shuffle: true
            splits:
                - fraction: 0.7
                  output: $tmp/train_raw
                - fraction: 0.1
                  output: $tmp/val_raw
                - fraction:
                  output: $tmp/test_raw
            grabber: $var(nproc, default=4)
    train_areas:
        map:
            stage:
                entity: iris-areas
                input: $tmp/train_raw
                output: $var(train)
                grabber: $var(nproc, default=4)
    val_areas:
        map:
            stage:
                entity: iris-areas
                input: $tmp/val_raw
                output: $var(val)
                grabber: $var(nproc, default=4)
    test_areas:
        map:
            stage:
                entity: iris-areas
                input: $tmp/test_raw
                output: $var(test)
                grabber: $var(nproc, default=4)
```

```{hint}
In this tutorial we have just scratched the surface of the Pipelime entity and action framework.
If you want to learn how deploy full-fledge classes as actions, overwrite or drop input items,
declare optional fields, use dynamic names for item keys, etc.
please take a look at the [full documentation](../../operations/stages.md).
```
