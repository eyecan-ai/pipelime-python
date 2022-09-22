# Commands

In this section you will learn how exploit the full potential of pipelime commands in your project.
Pipelime commands can be seen as packaged tasks that can be run individually or as part of an execution graph.
You can delegate to [pydantic](https://pydantic-docs.helpmanual.io/) the tedious task of parsing
and validating the arguments, while pipelime takes care of the rest, namely:
- merging arguments from different sources (e.g., command line and config file)
- advanced configuration management through [Choixe](../choixe/intro.md)
- easy multi-processing
- automatic cli and help generation for your project

We assume a basic understanding of [pydantic](https://pydantic-docs.helpmanual.io/),
which is key to get the most out of pipelime commands. However, as you will see common use cases are
already covered by pipelime, so you don't need to worry if you are not yet a pydantic power user!

## Relevant Modules

All commands derive from `PipelimeCommand`, which is defined in the `pipelime.piper` subpackage. In the same subpackage you can find the enum `PiperPortType`, which we will introduce later.
Some commands' arguments are often the same, e.g., the input and output datasets, therefore pipelime provides some general *interfaces* defined in `pipelime.commands.interfaces`.

Note that the `pipelime.commands` subpackage just imports the standard pipelime commands, so it is usually not necessary when you are writing you own.

## A Naive Implementation

Let's say we want to write a command that perform the following task:
1. loads a dataset of images
2. computes mean and standard deviation of the images
3. applies such values to all the images in the dataset
4. saves the result to a new folder

This might be a first attempt to implement it:

```python
from pydantic import Field
import numpy as np
from pipelime.piper import PipelimeCommand, PiperPortType
from pipelime.sequences import SamplesSequence
from pipelime.stages import StageLambda

class StandardizationCommand(PipelimeCommand, title="std-img"):
    """Standardize images by subtracting the mean and dividing
    by the standard deviation.
    """

    input: str = Field(
        ..., description="The input folder.", piper_port=PiperPortType.INPUT
    )
    output: str = Field(
        ..., description="The output folder.", piper_port=PiperPortType.OUTPUT
    )
    image_key: str = Field("image", description="The key of the input image.")
    out_std_image_key: str = Field(
        "std_image", description="The key of the output standardized image."
    )

    def run(self):
        # load the dataset
        seq = SamplesSequence.from_underfolder(self.input)

        # add a stage to get the images in the range [0, 1]
        seq = seq.map(
            StageLambda(
                lambda x: x.set_value_as(
                    self.out_std_image_key,
                    self.image_key,
                    x[self.image_key]().astype(float) / 255.0,
                )
            )
        )

        # cache the values, so that we don't need to recompute it every time
        seq = seq.cache()

        # compute global mean and std
        mean = 0
        counter = 0
        for x in seq:
            image = x[self.out_std_image_key]()
            mean += image.sum()
            counter += image.size
        mean /= counter

        stddev = 0
        for x in seq:
            image = x[self.out_std_image_key]()
            stddev += ((image - mean) ** 2).sum()
        stddev = np.sqrt(stddev / (counter-1))

        # apply the standardization and save the result
        seq = seq.map(
            StageLambda(
                lambda x: x.set_value(
                    self.out_std_image_key,
                    (
                        ((x[self.out_std_image_key]() - mean) / stddev) * 128.0 + 128.0
                    ).clip(0, 255).astype(np.uint8),
                ),
            )
        )

        seq = seq.to_underfolder(self.output)
        seq.run()
```

First, note that all parameters are defined as pydantic `Field`s with a `description` and, optionally, a default value. Moreover, the `input` and `output` fields are marked as `PiperPortType.INPUT` and `PiperPortType.OUTPUT`, respectively: this is needed to find dependencies between commands when building an [execution graph](../cli/piper.md).

Though this is a working implementation, it has some drawbacks, namely:
- there is no option to run the command in parallel
- there is no progress bar
- input and output cannot be [validated](../advanced/validation.md)

All these issues can be addressed by using built-in interfaces from pipelime.

## Using The Interfaces

Common options, such as the input and output datasets, can be easily deployed in a standard way by using built-in interfaces from pipelime.
For example, the previous command can be rewritten as follows:

```python
from pydantic import Field
import numpy as np
from pipelime.piper import PipelimeCommand, PiperPortType
from pipelime.sequences import SamplesSequence
from pipelime.stages import StageLambda
import pipelime.commands.interfaces as plint

class StandardizationCommand(PipelimeCommand, title="std-img"):
    """Standardize images by subtracting the mean and dividing
    by the standard deviation.
    """

    input: plint.InputDatasetInterface = (
        plint.InputDatasetInterface.pyd_field(alias="i", piper_port=PiperPortType.INPUT)
    )
    output: plint.OutputDatasetInterface = (
        plint.OutputDatasetInterface.pyd_field(
            alias="o", piper_port=PiperPortType.OUTPUT
        )
    )
    image_key: str = Field("image", alias="i", description="The key of the input image.")
    out_std_image_key: str = Field(
        "std_image", alias="s", description="The key of the output standardized image."
    )
    grabber: plint.GrabberInterface = plint.GrabberInterface.pyd_field(alias="g")

    def run(self):
        # load the dataset
        seq = self.input.create_reader()

        # add a stage to get the images in the range [0, 1]
        seq = seq.map(
            StageLambda(
                lambda x: x.set_value_as(
                    self.out_std_image_key,
                    self.image_key,
                    x[self.image_key]().astype(float) / 255.0,
                )
            )
        )

        # cache the values, so that we don't need to recompute it every time
        seq = seq.cache()

        # compute global mean and std
        mean = 0
        counter = 0
        for x in seq:
            image = x[self.out_std_image_key]()
            mean += image.sum()
            counter += image.size
        mean /= counter

        stddev = 0
        for x in seq:
            image = x[self.out_std_image_key]()
            stddev += ((image - mean) ** 2).sum()
        stddev = np.sqrt(stddev / (counter-1))

        # apply the standardization
        seq = seq.map(
            StageLambda(
                lambda x: x.set_value(
                    self.out_std_image_key,
                    (
                        ((x[self.out_std_image_key]() - mean) / stddev) * 128.0 + 128.0
                    ).clip(0, 255).astype(np.uint8),
                ),
            )
        )

        # write to disk
        seq = self.output.append_writer(seq)
        self.grabber.grab_all(
            seq,
            grab_context_manager=self.output.serialization_cm(),
            keep_order=False,
            parent_cmd=self,
            track_message=f"Saving standardized images ({len(seq)} samples)",
        )
```

In the new implementation above, a number of improvements and best practices are adopted:
1. Fields include an `alias` to add an alternative (short) name. For example, the `input` field can be specified as `i` or `input`.
2. `input` and `output` are now pipelime interfaces. This gives the user a standard way to specify more than just the folder, including a validation schema (see section [CLI](../cli/overview.md) for more details).
3. The interfaces provide utility methods to easily create a reader (`self.input.create_reader()`) and append a writer to a sequence (`self.output.append_writer(seq)`). Also, the `grabber` interface allows to run on multiple processes and to show a progress bar.
4. Note how the serialization options are given as a **context manager** to the `grabber` interface (`self.output.serialization_cm()`). This ensure that they are correctly applied even when multiple processes are involved.

## Common Built-in Interfaces

When writing a new command, you should always use the built-in interfaces for common options.
This way, users feel comfortable with your CLI and new features are automatically available to your command, without the need to change the code.
Moreover, most interfaces are able to parse inputs in the so-called *compact form*, i.e., instead of a key-value mapping, just a single string that contains all the information needed to create the object.

In the following a short list of the most common interfaces available.

### GrabberInterface

It exposes options to setup a multiprocessing run. Relevant methods:
- `pyd_field`: a class method to ease the creation of the corresponding pydantic field.
- `grab_all`, `grab_all_wrk_init`: methods to run a sequence on multiple processes. It takes care of showing a progress bar and of correctly handling the serialization options.

Compact form: `num_workers[,prefetch]`. Examples:
- `pipelime clone +g 4`: run on 4 processes with default prefetch
- `pipelime clone +g 6,12`: run on 6 processes with 12 samples prefetched by each process

### InputDatasetInterface

A general interface to load a dataset from disk. Relevant methods:
- `pyd_field`: a class method to ease the creation of the corresponding pydantic field.
- `create_reader`: a method to create a reader for the dataset.

Input data can be validated against a schema, see [Validation](../advanced/validation.md) for more details.

Compact form: `folder[,skip_empty]`. Examples:
- `pipelime clone +i path`: read all samples from the folder `path`
- `pipelime clone +i path,true`: read only non-empty samples from the folder `path`

### OutputDatasetInterface

A general interface to save a dataset to disk. Relevant methods:
- `pyd_field`: a class method to ease the creation of the corresponding pydantic field.
- `append_writer`: a method to append a writer to a sequence.
- `serialization_cm`: a method to create a context manager to handle the serialization options in a multiprocessing run.
- `as_pipe`: a method to get a dictionary to add to a pipe list, useful, e.g., when working with [data streams](../advanced/data_streaming.md).
