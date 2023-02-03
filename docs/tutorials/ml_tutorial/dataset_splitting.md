# Dataset Splitting

In this tutorial andiamo avanti per preparare i dati per addestrare un sistema di ML


In this tutorial we will train a multi-layer perceptron (MLP) network to classify the Iris dataset we used in the [previous tutorial](./convert_to_underfolder.md). You will learn how to:
* write a simple DAG to prepare your data
* create a new Pipelime Command to train your model
* use entities and actions to define a new stage for inference
* put all together, draw the final DAG and track the execution

## Train, Validation and Test Sets

To train and test a model we need to split the dataset into three sets:
* 70% of data for training
* 10% of data for validation
* 20% of data for testing

To accomplish this task, Pipelime provides the `split` command. Let's see the documentation:

```bash
$ pipelime help split -v
```

* `input` / `i`: the standard input dataset interface
* `shuffle` / `shf`: shuffle the dataset _before_ subsampling and splitting
* `subsample` / `ss`: take 1-every-nth sample _after_ shuffling
* `splits` / `s`: a split to create given as `fraction,output`
* `grabber` / `g`: optional parallel grabbing of the samples

```{tip}
To test or debug the command line, you can use the switch `-d` (dry run) and `-v` (verbose) to see how the options are parsed. Use `-v` multiple times to get even more information.
```

To generate and split the iris dataset in one call, we put everything together in a direct acyclic graph (DAG) and run it through **Piper**, a powerful Pipelime component. To do this, we will make use of the `run` command and `$ pipelime help run -v` tells us how to properly write a configuration:
1. `nodes` is the dictionary mapping node names to Pipelime commands
1. `include` and `exclude` allow to include or exclude nodes from the execution
1. `token` and `watch` are used to track the execution

Options (2) and (3) can be used to customize the execution from command line.
Instead, we put all the nodes in the configuration file:

```yaml
nodes:
    generate:
        pipe:
            operations: iris
            output: path/to/iris/folder
            grabber: 4
    data_split:
        split:
            input: path/to/iris/folder
            shuffle: true
            splits:
                - fraction: 0.7
                  output: path/to/iris/train
                - fraction: 0.1
                  output: path/to/iris/val
                - fraction:
                  output: path/to/iris/test
            grabber: 4
```

```{note}
The `fraction` option for the last split has been intentionally omitted, so that all the remaining samples will be assigned to it, with no rounding errors.
```

As you can see, there are many shortcomings:
1. samples are copied from the original dataset to the splits, wasting space and time
1. intermediate results, such as the dataset before splitting, should not be retained
1. paths should not be hardcoded
1. some options should have a default the user can change

Actually, the first issue is already solved by Pipelime itself, since Items are _hardlinked_ by default whenever possible. This means that as long as the original file and its copy are on the same partition, hardlinks are _the same file with a different name_, so no extra space is required and their creation takes no time.

As for the other points, they can be addressed with **Choixe**.

## Choixe: Directives, Variables And Context

Choixe is a Pipelime module to level up the configuration management with variables, loops, object creation, import directives and more.
In this tutorial you will learn the basic usage and the most common use cases.

First, to remove the intermediate results, you can leverage the `$tmp` directive to create a temporary folder that will be deleted at the end of the execution:

```yaml
nodes:
    generate:
        pipe:
            operations: iris
            output: $tmp
            grabber: 4
    data_split:
        split:
            input: $tmp
            shuffle: true
            splits:
                - fraction: 0.7
                  output: path/to/iris/train
                - fraction: 0.1
                  output: path/to/iris/val
                - fraction:
                  output: path/to/iris/test
            grabber: 4
```

Then, to remove the hardcoded paths, you might just leave them empty and force the user to specify them from the command line. However, this is not very user-friendly, because the user has to remember the exact option tree to insert, eg, `++nodes.data_split.split.splits[0].output path/to/train`. Instead, you can create a variable through the `$var` directive:

```yaml
nodes:
    generate:
        pipe:
            operations: iris
            output: $tmp
            grabber: 4
    data_split:
        split:
            input: $tmp
            shuffle: true
            splits:
                - fraction: 0.7
                  output: $var(train)
                - fraction: 0.1
                  output: $var(val)
                - fraction:
                  output: $var(test)
            grabber: 4
```

and then set them from the command line using the special `@` prefix, eg, `@train path/to/train`. Likewise, a `$var` can expose key parameters of a DAG, such as the number of processors to use for parallel grabbing, together with a default value:

```yaml
nodes:
    generate:
        pipe:
            operations: iris
            output: $tmp
            grabber: $var(nproc, default=4)
    data_split:
        split:
            input: $tmp
            shuffle: true
            splits:
                - fraction: 0.7
                  output: $var(train)
                - fraction: 0.1
                  output: $var(val)
                - fraction:
                  output: $var(test)
            grabber: $var(nproc, default=4)
```

The benefit of using variables is that a user can inquire a configuration file to get a list of them. Copy the yaml above to a file named `iris.yaml` and run:

```bash
$ pipelime audit -c iris.yaml
>
📄 CONFIGURATION AUDIT

*** /path/to/iris.yaml
🔍 imports:
🔍 variables:
{
    'nproc': 4,
    'train': None,
    'val': None,
    'test': None
}
🔍 help_strings:
🔍 environ:
🔍 symbols:
🔍 processed:
False
*** command line
🔍 imports:
🔍 variables:
🔍 help_strings:
🔍 environ:
🔍 symbols:
🔍 processed:
True

📄 CONTEXT AUDIT

{}

WARNING: Some variables are not defined in the context.
ERROR: Invalid configuration! Variable not found: `train`
```

The dictionary under `variables` is what you need to create your context:

```yaml
nproc: 4
train: path/to/train
val: path/to/val
test: path/to/test
```

Copy this yaml to `iris_context.yaml` _in the same configuration folder_ and see how variable substitution works:

```bash
$ pipelime run -c iris.yaml -dv
```

```{tip}
If the context is in a different folder or its name does not contain the word "context", you can specify it one or more times with the `-x` option.
```

```{hint}
You can always mix configuration files, context files and command line options. All sources are merged together following the order in which they are listed. Command line options are merged last, so they can potentially overwrite any parameter.
```
