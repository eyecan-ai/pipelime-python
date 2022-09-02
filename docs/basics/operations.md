# Operations

Pipelime offers many tools to process datasets in many different ways. This is very useful to create custom automated data pipelines.

## Stages

Stages (subclasses of `SampleStage`) are the simplest form of dataset processing. They are applied independently to all samples of a sample sequence. The following constraints apply:

- They are applied only to a single sequence (but you can easily merge or concatenate them if needed!). 
- The input and output sequences have the same length.
- Each sample of the output sequence should depend uniquely on the corresponding sample of the input sequence.

If possible, you should prefer this kind of operation to the others because it is much easier to implement and it is automatically parallelized.

Stages are applied through the `map` method and multiple stages can be applied sequentially with a `StageCompose`:

```{python}
from pipelime.sequences import SamplesSequence
from pipelime.stages import StageKeysFilter

# Create a dataset of 10 samples
dataset = SamplesSequence.toy_dataset(10)

# Attach the stage
dataset = dataset.map(StageKeysFilter(key_list=["image", "mask"]))

# Apply to all the samples, possibly using multiple processes
dataset = dataset.apply(num_workers=4)
```

## Pipes

If stages are too limited to implement your custom operation, you should consider piped sequences. Piped sequences follow the decorator pattern, i.e. they wrap another sample sequence (the source) and provide an alterated view of the source sequence. This way, from the outside they look like a modified version of the original sequence, but in reality no data is actually modified. Also, despite being subclasses of `SamplesSequence`, they can be created just by calling a method on the base class, so that multiple operations are easily chained together:

```{python}
from pipelime.sequences import SamplesSequence

# Create a dataset of 10 samples
dataset = SamplesSequence.toy_dataset(10)

# Chain multiple operations
dataset = dataset.repeat(100).shuffle()

# Slicing is another useful operation
dataset = dataset[20:40:2]
```

What if you want to load/dump your sequence from/to yaml/json? Just call `to_pipe` and `build_pipe` functions:

```{python}
from pipelime.sequences import SamplesSequence, build_pipe
import yaml

# Create a dataset
dataset = SamplesSequence.toy_dataset(10).repeat(100).shuffle()

# Serialize
pipe = dataset.to_pipe()
pipe_str = yaml.dump(pipe)

# De-serialize
pipe = yaml.safe_load(pipe_str)
dataset = build_pipe(pipe)
```

Unlike stages, parallelization for PipedSequences is only partially automatic, i.e., it is limited to the data access. You may want to parallelize other methods, e.g., the `__init__()` call.

## Commands

Commands are the most generic form of operation: they can accept and return any number of sequences of different length. No constraints are applied, but you will have to manually implement most of the things that come for free with the other type of operations. Don't worry though, pipelime provides all the necessary tools to read, process, write, track your progress and even generate a CLI for your commands, so the amount of code to write will still be minimal.

Since commands can have multiple inputs/outputs, they are not as easy to compose as stages or pipes (which can really just be concatenated), instead, their composition results in a Directed Acyclic Graph (DAG). Luckily for you, pipelime lets you easily define custom DAGs to fully automate even complex data pipelines. DAGs can be defined in a yaml/json file with the help of Choixe.

## Recap

Pipelime offers a wide variety of built-in generic stages, pipes and commands that come helpful in many situations to minimize your effort. In 99% of the times you will just need to implement some custom stages, which is as simple as defining a python function, pipelime will take care of all the boilerplate for you, letting you never lose focus on the core logic of your data pipeline.

|                      | Stage | Pipe | Command |
| -------------------- | ----- | ---- | ------- |
| Auto Parallelization | ✅     | ❌    | ❌       |
| Auto I/O             | ✅     | ✅    | ❌       |
| Auto Tracking        | ✅     | ✅    | ❌       |
| Auto CLI generation  | ✅     | ✅    | ✅       |

