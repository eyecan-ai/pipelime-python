# Operations

Pipelime offers different tools to process datasets in many different ways. This is very useful to create custom automated data pipelines. 

## Stages

Stages (subclasses of `SampleStage`) are the simplest form of dataset processing. They are applied independently to all samples of a sample sequence. The following constraints apply:

- They accept and return a single sequence. 
- The input and output have the same length.
- Each sample of the output sequence depends uniquely on the corresponding sample of the input sequence.

If possible, you should prefer this kind of operation to the others because it is much easier to implement and it is automatically parallelized.

Multiple stages can be applied sequentally with a `StageCompose`.

## Pipes

If stages are too limited to implement your custom operation, you should consider piped sequences. Piped sequences follow the decorator pattern, i.e. they wrap another sample sequence (the source) and provide an alterated view of the source sequence. This way, from the outside they look like a modified version of the original sequence, but in reality no data is actually modified.

Like stages, `PipedSequences` can be composed sequentially by calling the `build_pipe` function. 

Unlike stages, parallelization for PipedSequences is only partially automatic. You may need to manually parallelize some parts of your code.

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

