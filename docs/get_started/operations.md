# Operations

Pipelime offers many tools to process datasets in different ways and to create custom automated data pipelines.
In this walkthrough, first we will see how to generate a new sequence, then how to chain operations together to create a pipeline.
Also, we will quickly cover some advanced topic, such as execution graphs and command line interface.

## Generators

First, a sequence must be generated calling a static method on `SamplesSequence`, eg:
- `from_underfolder`: reads a dataset in underfolder format
- `toy_dataset`: creates a simple toy dataset
- `from_list`: creates a dataset from a list of samples
- `from_callable`: delegates to a user-provided callable

A typical source is the [underfolder dataset format](underfolder.md). In its simplest form you just need to provide a path to the dataset folder:

```python
from pipelime.sequences import SamplesSequence

dataset = SamplesSequence.from_underfolder("datasets/mini_mnist")
```

However, you can also specify a bunch of options:

```python
from pipelime.sequences import SamplesSequence

dataset = SamplesSequence.from_underfolder(
    folder="datasets/mini_mnist",
    merge_root_items=True,
    must_exist=True,
    watch=False,
)
```

Here a brief description of the arguments above:
- `folder`: the path to the underfolder dataset, the keyword can be omitted
- `merge_root_items`: if `True`, the root items of the dataset are merged with each sample
- `must_exist`: if `True`, an error is raised if the dataset folder does not exist
- `watch`: if `True`, the dataset is watched for changes every time a new sample is requested

Another useful generator is `toy_dataset`:

```python
from pipelime.sequences import SamplesSequence

dataset = SamplesSequence.toy_dataset(
    length=10,

    with_images=True,
    with_masks=True,
    with_instances=True,
    with_objects=True,
    with_bboxes=True,
    with_kpts=True,

    image_size=256,
    key_format="*",

    max_labels=5,
    objects_range=(1, 5),
)
```

The `length` is number of samples to generate, while `with_*` arguments specify which data to include in each sample.
The `image_size` is the size of the generated images, which can be a single integer or a pair of values, while `key_format` is the format of the item keys, where any `*` is replaced with the base name of the key.
The `max_labels` is the maximum number of object labels in the dataset, while `objects_range` is a tuple of the minimum (inclusive) and maximum (exclusive) number of objects to generate for each sample.

## Pipes

Piped sequences follow the decorator pattern, i.e., they wrap another sample sequence (the source) and provide an alterated view of the source sequence.
You can attach and chain multiple pipes following the functional pattern:

```python
from pipelime.sequences import SamplesSequence

# Create a dataset of 10 samples
dataset = SamplesSequence.toy_dataset(10)

# Chain multiple pipes
piped_dataset = dataset.repeat(100).shuffle()
```

No pipe makes changes to the source dataset, but only provides a new view of it.
In the example above, you can still access the original data (not repeated nor shuffled) through the `dataset` variable.
Also, most pipes are designed to be *lazy* as much as possible, i.e., they defer the computation when an item is requested, instead of filling up the `__init__` method with heavy computations. This way, they can take advantage of the multiprocessing capabilities of the `SamplesSequence` class (see below).

These are the most common pipes:
- `to_underfolder`: writes sample to disk in underfolder format
- `map`: applies a [stage](#stages) to each sample (see below)
- `zip`: merges samples from two sequences
- `cat`: concatenates samples
- `filter`: applies a filter on samples, possibly reducing the length
- `sort`: sorts samples by a [key-function](https://docs.python.org/3/howto/sorting.html#key-functions)
- `shuffle`: puts the sample in random order
- `enumerate`: adds an item to each sample with its index within the sequence
- `repeat`: repeats the same sequence a given number of times
- `cache`: the first time a sample is accessed, it's value is written to a cache folder
- `no_data_cache`: disables item data caching on previous steps
- `data_cache`: enables item data caching on previous steps

Moreover, to filter the samples by index you can pass a list to `dataset.select([2,9,14])` or simply extract a slice as `dataset[start:stop:step]`.

Now it's time to talk about `map` and stages, but if you are eager to create your own sequence generator or pipe, jump to [Pipes](../operations/pipes.md).

## Stages

A special piped operation is `map`. Every time you get a sample from a mapped sequence, such sample is passed to a **stage** for further processing.
Stages are classes derived from `SampleStage` and are built to process samples independently. Therefore, they have some limitations:

- They are applied only to a single sequence (but you can easily merge or concatenate them if needed!).
- The input and output sequences have the same length (use `filter/select/[start:stop:step]` to remove samples).
- Each sample of the output sequence should depend solely on the corresponding sample of the input sequence.

Pipelime provides some common stages off-the-shelf:
- `compose`: applies a sequence of stages
- `identity`: returns the input sample
- `lambda`: applies a callable to the sample
- `filter-keys`: filters sample keys
- `remap-key`: remaps keys in sample preserving internal values
- `replace-item`: replaces items in sample preserving internal values
- `format-key`: changes key names following a format string
- `albumentations`: sample augmentation via [Albumentations](https://albumentations.ai/).

Since a stage is called by `map` only when you get a sample from the sequence, to actually process each sample you have to go through all of them:

```python
from pipelime.sequences import SamplesSequence
from pipelime.stages import StageKeysFilter

# Create a dataset of 10 samples
dataset = SamplesSequence.toy_dataset(10)

# Attach the stage
dataset = dataset.map(StageKeysFilter(key_list=["image", "mask"]))

# Alternatively, use its title
dataset = dataset.map({"filter-keys": {"key_list": ["image", "mask"]}})

# Naive approach to process all the samples
for sample in dataset:
    pass
```

However, if all you want is a new processed dataset, just call `apply`, possibly spawning multiple processes:

```python
from pipelime.sequences import SamplesSequence
from pipelime.stages import StageKeysFilter

dataset = SamplesSequence.toy_dataset(10)
dataset = dataset.map(StageKeysFilter(key_list=["image", "mask"]))

# Apply to all the samples, possibly using multiple processes
dataset = dataset.apply(num_workers=4)
```

Similarly, if you want to run a function on the main process, e.g., to collect global statistics while grabbing the sample with multiple processes, you can use the `run` method:

```python
from pipelime.sequences import SamplesSequence
from pipelime.stages import StageKeysFilter
import numpy as np

dataset = SamplesSequence.toy_dataset(10)
dataset = dataset.map(StageKeysFilter(key_list=["image", "mask"]))

# Counts the number of valid samples
counter = 0

# This function will be called synchronously on the main process
def _count_valid_samples(x):
    if not np.any(x["mask"]()):
        counter += 1

dataset.run(num_workers=4, sample_fn=_count_valid_sample)
```

Checkout section [Stages](../operations/stages.md) to see how to create a custom stage.

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

## Commands

Complex operations, which may include some sort of data processing, may be easily linked in a
[Directed Acyclic Graph (DAG)](../piper/dags.md)
and executed with the help of **Piper**, a Pipelime's core component.
Using Piper and [**Choixe**](../choixe/intro.md), another Pipelime's core component,
you can create a graph of commands to execute with an associated user-defined configuration.
Such commands are classes derived from `PipelimeCommand` and they include a simple way to track the progress either
through a progress bar or sending updates over a socket.

Here a list of the most common commands available:
- `run`: executes a DAG
- `draw`: draws a DAG
- `pipe`: applies a sequence of operations to a dataset
- `clone`: clones a dataset, e.g., to download the data from a remote data lake
- `cat`: concatenates two or more datasets
- `split/split-value/split-query`: various dataset splitting commands
- `toy_dataset`: creates a toy dataset
- `shell`: run any shell command
- `timeit`: measure the time to get a sample from a sequence

Commands are executed from the shell with the `pipelime` command, e.g.,

```bash
$ pipelime clone +i path/to/input +o path/to/output,false,true +g 4
```

Using the command line interface is straighforward once you know the rationale behind it,
which is described in section [CLI](../cli/cli.md).

Commands are just classes ([pydantic](https://pydantic-docs.helpmanual.io) models, specifically),
so you can also create and run them programmatically:

```python
from pipelime.commands import ConcatCommand

cmd = ConcatCommand(
    inputs=["dataset1", "dataset2", "dataset3", "dataset4"],
    output="cat_dataset",
)
cmd()
```

Beware that `inputs` and `output` here are *interfaces* to command options, so they have to be defined in a special way (see section [CLI](../cli/cli.md) for more details).

```{admonition} TIP
:class: tip

Commands are a really powerful tool to create complex pipelines and also to easily add to any project a complete CLI with rich help, advanced configuration management e automatic multiple-processing!

All you have to do is to pack your code as `PipelimeCommand`s and `SampleStage`s - and let Pipelime do the rest!

See [Cli](../cli/cli.md) for more details.
```
