# Operations

Pipelime offers many tools to process datasets in many different ways. This is very useful to create custom automated data pipelines.

## Generators

First, a sequence must be generated calling a static method on `SamplesSequence`, eg:
- `from_underfolder`: reads a dataset in underfolder format
- `toy_dataset`: creates a simple toy dataset
- `from_list`: creates a dataset from a list of samples
- `from_callable`: delegates to a user-provided callable

If you want to provide access to your own data, you should consider to use `SamplesSequence.from_callable()`, so that you just have to provide a function `(idx: int) -> Sample`. However, implementing a new generator is not too difficult. First, derive from `SamplesSequence`, then:
1. apply the decorator `@source_sequence` to your class
2. set a `title`: this will be the name of the associated method (see the example below)
3. provide a class help: it will be used for automatic help generation (see [Cli](../cli/cli.md))
4. define your parameters as [`pydantic.Field`](https://pydantic-docs.helpmanual.io/) (Field's description will be used for automatic help generation)
5. implement `def get_sample(self, idx: int) -> Sample` and `def size(self) -> int`

```python
from typing import List
from pathlib import Path
from pydantic import Field, DirectoryPath, PrivateAttr
from pipelime.sequences import SamplesSequence, Sample, source_sequence
from pipelime.items.base import ItemFactory

@source_sequence
class SequenceFromImageList(pls.SamplesSequence, title="from_image_list"):
    """A SamplesSequence loading images in folder as Samples."""

    folder: DirectoryPath = Field(..., description="The folder to read.")
    ext: str = Field(".png", description="The image file extension.")

    _samples: List[Path] = PrivateAttr()

    def __init__(self, **data):
        super().__init__(**data)
        self._samples = [
            Sample({"image": ItemFactory.get_instance(p)})
            for p in self.folder.glob("*" + self.ext)
        ]

    def size(self) -> int:
        return len(self._samples)

    def get_sample(self, idx: int) -> pls.Sample:
        return self._samples[idx]
```

## Pipes

Piped sequences follow the decorator pattern, i.e., they wrap another sample sequence (the source) and provide an alterated view of the source sequence.
You can attach and chain multiple operations following a functional pattern:

```python
from pipelime.sequences import SamplesSequence

# Create a dataset of 10 samples
dataset = SamplesSequence.toy_dataset(10)

# Chain multiple operations
dataset = dataset.repeat(100).shuffle()
```

These are the most common operations:
- `to_underfolder`: writes sample to disk in underfolder format
- `map`: applies a [stage](#stages) to each sample (see below)
- `zip`: merges samples from two sequences
- `cat`: concatenates samples
- `filter`: applies a filter on samples, possibly reducing the length
- `sort`
- `shuffle`
- `enumerate`: adds an item to each sample with the sample index
- `repeat`
- `cache`: the first time a sample is accessed, it's value is written to a cache folder

Moreover, to filter the indexes you can pass a list to `dataset.select([2,9,14])` or simply extract a slice as `dataset[start:stop:step]`.

To create your own piped operation just derive from `PipedSequenceBase`, then:
1. apply the decorator `@piped_sequence` to your class
2. set a `title`: this will be the name of the associated method (see the example below)
3. provide a class help: it will be used for automatic help generation (see [CLI](../cli/cli.md))
4. define your parameters as [`pydantic.Field`](https://pydantic-docs.helpmanual.io/) (Field's description will be used for automatic help generation)
5. implement `def get_sample(self, idx: int) -> Sample` and, possibly, `def size(self) -> int`

```python
from pydantic import Field

from pipelime.sequences import Sample, piped_sequence
from pipelime.sequences.pipes import PipedSequenceBase

@piped_sequence
class ReverseSequence(PipedSequenceBase, title="reversed"):
    """Reverses the order of the first `num` samples."""

    num: int = Field(..., description="The number of samples to reverse.")

    def get_sample(self, idx: int) -> Sample:
        if idx < self.num:
            return self.source[self.num - idx - 1]
        return self.source[idx]
```

Once the module has been imported, the operation above registers themself as `reversed` on `SamplesSequence`, so that you can simply do `dataset.reversed(20)`.

## Stages

A special piped operation is `map`. Every time you get a sample from a mapped sequence, such sample is passed to a **stage** for further processing.
Stages are classes derived from `SampleStage` and are built to process samples independently. Therefore, they have some limitations:

- They are applied only to a single sequence (but you can easily merge or concatenate them if needed!).
- The input and output sequences have the same length (use `filter/select/[start:stop:step]` to remove samples).
- Each sample of the output sequence should depend solely on the corresponding sample of the input sequence.

Pipelime provides some common stages off-the-shelf:
- compose: applies a sequence of stages
- identity: returns the input sample
- lambda: applies a callable to the sample
- filter-keys: filters sample keys
- remap-key: remaps keys in sample preserving internal values
- replace-item: replaces items in sample preserving internal values
- format-key: changes key names following a format string
- albumentations: sample augmentation via [Albumentations](https://albumentations.ai/).

Since a stage is called by `map` only when you get a sample from the sequence, to actually process each sample you have to go through all of them:

```python
from pipelime.sequences import SamplesSequence
from pipelime.stages import StageKeysFilter

# Create a dataset of 10 samples
dataset = SamplesSequence.toy_dataset(10)

# Attach the stage
dataset = dataset.map(StageKeysFilter(key_list=["image", "mask"]))

# Naive approach to process all the samples
for sample in dataset:
    do_something_with_the_filtered_sample(sample)
```

However, if all you want is a new processed dataset, just call `apply`, possibly spawning multiple processes:

```python
from pipelime.sequences import SamplesSequence
from pipelime.stages import StageKeysFilter

# Create a dataset of 10 samples
dataset = SamplesSequence.toy_dataset(10)

# Attach the stage
dataset = dataset.map(StageKeysFilter(key_list=["image", "mask"]))

# Alternatively, use its title
dataset = dataset.map({"filter-keys": {"key_list": ["image", "mask"]}})

# Apply to all the samples, possibly using multiple processes
dataset = dataset.apply(num_workers=4)
```

Similarly, if you want to run a function on the main process, e.g., to collect global statistics while grabbing the sample with multiple processes, you can use the `run` method:

```python
from pipelime.sequences import SamplesSequence
from pipelime.stages import StageKeysFilter
import numpy as np

# Create a dataset of 10 samples
dataset = SamplesSequence.toy_dataset(10)

# Attach the stage
dataset = dataset.map(StageKeysFilter(key_list=["image", "mask"]))

# Alternatively, use its title
dataset = dataset.map({"filter-keys": {"key_list": ["image", "mask"]}})

counter = 0

# This function will be called synchronously on the main process
def _count_valid_samples(x):
    if not np.any(x["mask"]()):
        counter += 1

dataset.run(num_workers=4, sample_fn=_count_valid_sample)
```

## De/Serialization

What if you want to load/dump your sequence from/to yaml/json? Just call `to_pipe` and `build_pipe` functions:

```python
from pipelime.sequences import SamplesSequence, build_pipe
import yaml

# Create a dataset and chain some operations
dataset = SamplesSequence.toy_dataset(10).repeat(100).shuffle()

# Serialize to yaml
pipe = dataset.to_pipe()
pipe_str = yaml.dump(pipe)

# De-serialize from yaml
pipe = yaml.safe_load(pipe_str)
dataset = build_pipe(pipe)
```

## Piper

Complex operations, which may include some sort of data processing, may be easily linked in a Directed Acyclic Graph (DAG)
and executed with the help of Piper, a Pipelime's core component.
Using Piper and Choixe, another Pipelime's core component, you can create a graph of commands to execute with an associated configuration.
Such commands are classes derived from `PipelimeCommand` and provide off-the-shelf a simple way to track the progress either
through a progress bar or sending updates over a socket.

Here a list of most common commands available:
- `run`: executes a DAG
- `draw`: draws a DAG
- `pipe`: applies a sequence of operations to a dataset
- `clone`: clones a dataset, e.g., to download the data from a remote data lake
- `cat`: concatenates two or more datasets
- `split/split-value/split-query`: various dataset splitting commands
- `toy_dataset`: creates a toy dataset
- `shell`: run any shell command
- `timeit`: measure the time to get a sample from a sequence

However, the power of Pipelime does not end here. The same command classes you use to create your dags,
are exposed from Pipelime's command line interface (CLI) as callable commands. Therefore, in any project you can create a complete
CLI with rich help, advanced configuration management e automatic multiple-processing by just writing your scripts as `PipelimeCommand`!
See [Cli](../cli/cli.md) for more details.

## Recap

Pipelime offers a wide variety of built-in generic stages, pipes and commands that come helpful in many situations to minimize your effort.
More often than not you will just need to implement some custom stages, which is as simple as defining a python function, while Pipelime will take care of all the boilerplate for you, letting you never lose focus on the core logic of your data pipeline.
