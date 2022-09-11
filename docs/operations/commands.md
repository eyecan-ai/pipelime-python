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

First, note that all parameters are defined as pydantic `Field`s with a `description` and, optionally, a default value. Moreover, the `input` and `output` fields are marked as `PiperPortType.INPUT` and `PiperPortType.OUTPUT`, respectively: this is needed to find dependencies between commands when building an [execution graph](../piper/dags.md).

Though this is a working implementation, it has some drawbacks, namely:
- there is no option to run the command in parallel
- there is no progress bar
- input and output cannot be [validated](../advanced/validation.md)

All these issues can be addressed by using built-in interfaces from pipelime.

## Using Interfaces
