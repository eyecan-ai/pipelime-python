# Directives

## Variables

Suppose you have a very long and complex configuration file, and you often need to change some values in it, depending on external factors. You can:

- Manually edit it each time, waste some time looking for the specific value to edit, keep a history of the changes or otherwise you won't be able to go back.
- Keep the files immutable but instead create a duplicate for every possible value, and when you eventually realize something was wrong with the original file, you have to propagate the changes in all the 20.000 copies you created.
- Replace the values you need to change with **Variables**, and let **Choixe** fill in the values for you, keeping only **one**, **immutable** version of the original file.

The following example consists in a toy configuration file for a deep learning training task, involving a moderate amount of parameters. Your configuration file looks like this:

```yaml
model:
  architecture:
    backbone: resnet18
    use_batch_norm: false
    heads:
      - type: classification
        num_classes: 10
      - type: classification
        num_classes: 7
training:
  device: cuda
  epochs: 100
  optimizer:
    type: Adam
    params:
      learning_rate: 0.001
      betas: [0.9, 0.99]
```

In this toy configuration file there are some parameters entirely dependant from the
task at hand. Take for instance the number of classes, whenever you decide to perform
a training on a different dataset, the number of classes is inevitably going to change.

To avoid the pitfalls described earlier, you can use **variables**. Think of a **variable**
as a placeholder for something that will be defined later. **Variables** values are picked at runtime from a structure referred to as **"context"**, that can be passed to **Choixe** python
API.

To use variables, simply replace a literal value with a `var` directive:

`$var(identifier: str, default: Optional[Any] = None, env: bool = False)`

Where:
- `identifier` is the [pydash](https://pydash.readthedocs.io/en/latest/deeppath.html) path (dot notation only) where the value is looked up in the **context**.
- `default` is a value to use when the context lookup fails - essentially making the variable entirely optional. Defaults to `None`.
- `env` is a bool that, if set to `True`, will also look at the system environment variables in case the **context** lookup fails. Defaults to `False`.
- `help` is a string containing an optional help message. Choixe uses this message when performing inspection to provide the user with a brief description. It serves no other purpose.

Here is what the deep learning toy configuration looks like after replacing some values with **variables**:

```yaml
model:
  architecture:
    backbone: resnet18
    use_batch_norm: $var(hparams.normalize, default=True)
    heads:
      - type: classification
        num_classes: $var(data.num_classes1) # No default: entirely task dependant
      - type: classification
        num_classes: $var(data.num_classes2) # No default: entirely task dependant
training:
  device: $var(TRAINING_DEVICE, default=cpu, env=True) # Choose device based on env vars.
  epochs: $var(hparams.num_epochs, default=100)
  optimizer:
    type: Adam
    params:
      learning_rate: $var(hparams.lr, default=0.001)
      betas: [0.9, 0.99]
```

The minimal **context** needed to use this configuration will look something like this:

```yaml
data:
  num_classes1: 10
  num_classes2: 7
```

The full context can contain all of these options:

```yaml
data:
  num_classes1: 10
  num_classes2: 7
hparams:
  normalize: true # Optional
  num_epochs: 100 # Optional
  lr: 0.001 # Optional
TRAINING_DEVICE: cuda # Optional, env
```

**Contexts** can also be seen as a "meta-configuration" providing an easier and cleaner access to a subset of "public" parameters of a templatized "private" configuration file with lots of details to keep hidden.

## Imports

Imagine having a configuration file in which some parts could be reused in other configuration files. It's not the best idea to duplicate them, instead, you can move those parts in a separate configuration file and dynamically import it using the `import` **directive**.

To use an import directive, replace any node of the configuration with the following directive:

`$import(path: str)`

Where:
  - `path` can be an absolute or relative path to another configuration file. If the path is relative, it will be resolved relatively from the parent folder of the importing configuration file, or, in case there is no importing file, the system current working directive.

Let's build on top of the previous "deep learning" example:

```yaml
model:
  architecture:
    backbone: resnet18
    use_batch_norm: $var(hparams.normalize, default=True)
    heads:
      - type: classification
        num_classes: $var(data.num_classes1)
      - type: classification
        num_classes: $var(data.num_classes2)
training:
  device: $var(TRAINING_DEVICE, default=cpu, env=True)
  epochs: $var(hparams.num_epochs, default=100)
  optimizer:
    type: Adam
    params:
      learning_rate: $var(hparams.lr, default=0.001)
      betas: [0.9, 0.99]
```

Here, one could choose to factor out the `optimizer` node and move it into a separate file called "adam.yml".

```yaml
# neural_network.yml
model:
  architecture:
    backbone: resnet18
    use_batch_norm: $var(hparams.normalize, default=True)
    heads:
      - type: classification
        num_classes: $var(data.num_classes1)
      - type: classification
        num_classes: $var(data.num_classes2)
training:
  device: $var(TRAINING_DEVICE, default=cpu, env=True)
  epochs: $var(hparams.num_epochs, default=100)
  optimizer: $import(adam.yml)
```

```yaml
# adam.yml
type: Adam
params:
  learning_rate: $var(hparams.lr, default=0.001)
  betas: [0.9, 0.99]
```

Note that "adam.yml" contains some **directives**. This is not a problem and it is handled automatically by **Choixe**. There is also no restriction on using **imports** in imported files, you can nest them as you please.

## Sweeps

**Sweeps** allow to perform an exhaustive search over a parametrization space, in a grid-like fashion, without having to manually duplicate the configuration file.

To use a `sweep` **directive**, replace any node of the configuration with the following **directive**:

`$sweep(*args: Any)`

Where:
- `args` is an arbitrary set of parameters.

All the **directives** introduced so far are "non-branching", i.e. they only have one possible outcome. **Sweeps** instead, are currently the only "branching" **Choixe directives**, as they produce multiple configurations as their output.

Example:

```yaml
foo:
  alpha: $sweep(a, b) # Sweep 1
  beta: $sweep(10, 20, hello) # Sweep 2
```

Will produce the following **six** outputs, the cartesian product of `{a, b}` and `{10, 20, hello}`:

1. ```yaml
   foo:
     alpha: a
     beta: 10
   ```
2. ```yaml
   foo:
     alpha: b
     beta: 10
   ```
1. ```yaml
   foo:
     alpha: a
     beta: 20
   ```
2. ```yaml
   foo:
     alpha: b
     beta: 20
   ```
3. ```yaml
   foo:
     alpha: a
     beta: hello
   ```
4. ```yaml
   foo:
     alpha: b
     beta: hello
   ```

```{figure} ../images/sweep_exec.svg
:width: 80%
:align: center
```

By default, all **sweeps** are global, each of them adds a new axis to the parameter space, regardless of the depth at which they appear in the structure. There is only one exception to this rule: if a **sweep** appears inside a branch of another sweep; in this case, the new axis is added locally.

Example:

```yaml
foo:
  $directive: sweep # Sweep 1 (global)
  $args:
    - alpha: $sweep(foo, bar) # Sweep 2 (local)
      beta: 10
    - gamma: hello
  $kwargs: {}
```

Will produce the following **three** outputs:

1. ```yaml
   foo:
     alpha: foo
     beta: 10
   ```
2. ```yaml
   foo:
     alpha: bar
     beta: 10
   ```
3. ```yaml
   foo:
     gamma: hello
   ```


```{figure} ../images/sweep_exec2.svg
:width: 70%
:align: center
```

## Instances
**Instances** allow to dynamically replace configuration nodes with real **python objects**. This can happen in two ways:
- With the `call` **directive** - dynamically import a python function, invoke it and replace the node content with the function result.
- With the `model` **directive** - dynamically import a [pydantic](https://docs.pydantic.dev/) `BaseModel` and use it to deserialize the content of the current node.

**Note**: these **directives** can only be used with the **special form**.

### Call

To invoke a python `Callable`, use the following directive.

```yaml
$call: SYMBOL [str]
$args: ARGUMENTS [dict]
```

Where:
- `SYMBOL` is a string containing either:
  - A filesystem path to a .py file, followed by `:` and the name of a callable.
  - A python module path followed by `.` and the name of a callable.
- `ARGUMENTS` is a dictionary containing the arguments to pass to the callable.

Example:

```yaml
foo:
  $call: path/to/my_file.py:MyClass
  $args:
    a: 10
    b: 20
```

Will import `MyClass` from `path/to/my_file.py` and produce the dictionary:

```python
{"foo": SomeClass(a=10, b=20)}
```

Another example:

```yaml
foo:
  $call: numpy.zeros
  $args:
    shape: [2, 3]
```

Will import `numpy.zeros` and produce the dictionary:

```python
{"foo": numpy.zeros(shape=[2, 3])}
```

### Model

Similar to `call`, the `model` **directive** will provide an easier interface to deserialize **pydantic** models.

The syntax is essentially the same as `call`:

```yaml
$model: SYMBOL [str]
$args: ARGUMENTS [dict]
```

In this case, there is the constraint that the imported class must be a `BaseModel` subtype.

### Symbol

If you want to import an already instanced python object, you can use the `symbol` **directive**. This **directive** is also available with a simple call form, instead of the more complex special forms of `call` and `model`.

`$symbol(symbol: str)`

Where `symbol` is a string in the same format used in `call` or `model`.

Example:

```yaml
pi: $symbol(numpy.pi)
```

## Loops

You want to repeat a configuration node, iterating over a list of values? You can do this with the `for` **directive**, available only with the following **key-value form**:

```yaml
$for(ITERABLE[, ID]): BODY
```

Where:
- `ITERABLE` can be either:
  - an **integer**: the loop will iterate from 0 to `ITERABLE - 1`.
  - a context key (just like a `var`) that points to an **integer**, as above.
  - a context key that points to a **list**: the loop will iterate over the items of this list.
- `ID` is an optional identifier for this for-loop, used to distinguish this specific loop from all the others, in case multiple loops are nested. Think of it like the `x` in `for x in my_list:`. When not specified, **Choixe** will use a random uuid behind the scenes.
- `BODY` can be either:
  - A **dictionary** - the for loop will perform dictionary union over all the iterations.
  - A **list** - the for loop will perform list concatenation over all the iterations.
  - A **string** - the for loop will perform string concatenation over all the iterations.

For-loops alone are not that powerful, but they are meant to be used along two other **directives**:

- `$index(identifier: Optional[str] = None)` or `$index`
- `$item(identifier: Optional[str] = None)` or `$item`

They, respectively, return the integer index and the item of the current loop iteration. If no identifier is specified (you can use the **compact form**), they will refer to the first for loop encountered in the stack. Otherwise, they will refer to the loop whose identifier matches the one specified.

Optionally, the `item` **directive** can contain a [pydash](https://pydash.readthedocs.io/en/latest/deeppath.html) key starting with the loop id, to refer to a specific item inside the structure.

Example:
```yaml
alice:
    # For loop that merges the resulting dictionaries
    "$for(params.cats, x)":
        cat_$index(x):
            index: I am cat number $index
            name: My name is $item(x.name)
            age: My age is $item(x.age)
bob:
    # For loop that extends the resulting list
    "$for(params.cats, x)":
        - I am cat number $index
        - My name is $item(x.name)
charlie:
    # For loop that concatenates the resulting strings
    "$for(params.cats, x)": "Cat_$index(x)=$item(x.age) "
```
Given the context:
```yaml
params:
  cats:
    - name: Luna
      age: 5
    - name: Milo
      age: 6
    - name: Oliver
      age: 14
```
Will result in:
```yaml
alice:
  cat_0:
    age: My age is 5
    index: I am cat number 0
    name: My name is Luna
  cat_1:
    age: My age is 6
    index: I am cat number 1
    name: My name is Milo
  cat_2:
    age: My age is 14
    index: I am cat number 2
    name: My name is Oliver
bob:
  - I am cat number 0
  - My name is Luna
  - I am cat number 1
  - My name is Milo
  - I am cat number 2
  - My name is Oliver
charlie: "Cat_0=5 Cat_1=6 Cat_2=14 "
```

## Switch Case

Choixe also supports switch-case-like control statements to change a configuration node based on the value of a context variable. This is especially useful for conditioned workflows and data pipelines.

Switch-case is only available with a **key-value form**.

```yaml
greeting_action:
  $switch(nation):
    # If the value is one of the following ...
    - $case: [UK, USA, Australia]
      $then: "Say 'hello'."

    # If the nation exactly matches ...
    - $case: Italy
      $then: "Say 'ciao'."

    # Optional default case
    - $default: "Just wave."
```

Based on different values from the context, the possible outcomes are the following:

```yaml
# For UK, USA and Australia
greeting_action: "Say 'hello'."
```

```yaml
# For Italy
greeting_action: "Say 'ciao'."
```

```yaml
# For everything else
greeting_action: "Just wave."
```

## Utilities

Along other more structural **directives**, **Choixe** provides some utilities commonly used when writing a configuration file.

### UUID

You can generate a random **uuid** with the `uuid` **directive**.

`$uuid`

Simple as that.

### Date

You can get the **current datetime** with the `date` **directive**.

`$date(format: str = None)`

Where format is the format string (see [python strftime format codes](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes) for more info). Defaults to isoformat.

### Command

You can get the output of a **system command** with the `cmd` **directive**.

`$cmd(command: str)`

Where command is a string with the command to run.

### Temporary Directories

You can get the path to a temporary directory with a custom name, on Linux, this results in the creation of a subfolder of `/tmp`.

`$tmp(name: str = None)`

Where name is the name of the subfolder. If no name is specified, a random name unique for this specific configuration will be generated.

### Random number generator

Choixe provides the `rand` directive to generate random numbers.
You can choose whether to generate single numbers, lists or numpy arrays; you can switch between float/int type and also control the distribution of random numbers using a PDF of your choice. Check out the "rand" examples section for a brief tutorial on how to use it.
