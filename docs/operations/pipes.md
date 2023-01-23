# Pipes And Generator

[We already saw](../get_started/entities.md) how to generate a sample sequence and chain pipes to it.
We will now detailed how you can extend the built-in types with your own generators and pipes.

## Custom Generators

If you want to provide access to your own data or create a synthetic dataset, you should consider to use `SamplesSequence.from_callable()`, so that all you have to do is to provide a function `(idx: int) -> Sample`.
However, writing a new generator class is not too difficult. First, derive from `SamplesSequence`, then:
1. apply the decorator `@source_sequence` to your class
2. set a `title`: this will be the name of the associated method (see the example below)
3. provide a class help: it will be used for automatic help generation (see [CLI](../cli/overview.md))
4. define your parameters as [`pydantic.Field`](https://docs.pydantic.dev/): field's description will be used for automatic help generation
5. implement `def get_sample(self, idx: int) -> Sample` and `def size(self) -> int`

```python
from typing import List
from pathlib import Path
from pydantic import Field, DirectoryPath, PrivateAttr
from pipelime.sequences import SamplesSequence, Sample, source_sequence
from pipelime.items.base import ItemFactory

@source_sequence
class SequenceFromImageList(SamplesSequence, title="from_image_list"):
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

    def get_sample(self, idx: int) -> Sample:
        return self._samples[idx]
```

In the above example notice that:
- we use `PrivateAttr` to define an internal variable (see [pydantic](https://docs.pydantic.dev/usage/models/#private-model-attributes) for details)
- we delegate to `ItemFactory.get_instance` the actual creation of the item: this way we support any possible extension as well as the [`.remote` files](../advanced/remotes.md)

Once the module is imported, the generator is automatically registered into `SamplesSequence`
as `from_image_list`:

```python
from pipelime.sequences import SamplesSequence

dataset = SamplesSequence.from_image_list(folder="path/to/folder")
```

Do you want a preview of the auto-generated help?
```python
from pipelime.cli import pl_print

pl_print("from_image_list")
```

```bash
>>>
                        from_image_list
  (*, folder: pydantic.types.DirectoryPath, ext: str = '.png')
     A SamplesSequence loading images in folder as Samples.
 Fields   Description                   Type            Default
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 folder   ▶ The folder to read.         DirectoryPath   ✗
 ext      ▶ The image file extension.   str             .png
```

## Custom Pipes

To create your own piped operation just derive from `PipedSequenceBase`, then:
1. apply the decorator `@piped_sequence` to your class
2. set a `title`: this will be the name of the associated method (see the example below)
3. provide a class help: it will be used for automatic help generation (see [CLI](../cli/overview.md))
4. define your parameters as [`pydantic.Field`](https://docs.pydantic.dev/) (Field's description will be used for automatic help generation)
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

In the above example notice that:
- we do not implement `size` since it does not change
- we refer to the source sequence as `self.source`; alternatively, we could have accessed the source by calling `super().get_sample()`

As with the generator before, once the module is imported, the pipe registers itself on `SamplesSequence`
with the given title, i.e., `reversed`, so that you can simply do, e.g., `dataset.reversed(num=20)`.

Do you want a preview of the auto-generated help?
```python
from pipelime.cli import pl_print

pl_print("reversed")
```

```bash
>>>
                            reversed
                         (*, num: int)
          Reverses the order of the first num samples.
 Fields   Description                           Type   Default
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 num      ▶ The number of samples to reverse.   int    ✗
```
