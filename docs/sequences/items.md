# Data Items

Item objects provide a general interface to efficiently manage the data in your pipeline.
Any item can be built from a local path or an URL to a remote data lake, as well as from a binary stream or simply raw data. Whatever the source, you should **never** directly access the internal attributes of an item, but always use the provided methods to access the data. This way, pipelime may ensure that, e.g., the data in memory actually represents the data on disk.

Though most of the class attributes is of no interest for the user, some are worth mentioning:
- `__call__`: returns the data wrapped by the item. This is the only method that should be used to access the data.
- `cache_data` (property): if `True` (the default) the data is cached in memory after the first access, if not already loaded.
- `serialization_mode` (property): the serialization mode to use when saving the item to disk (see below [Serialization Modes](#serialization-modes)).
- `is_shared` (property): if `True` the item is shared between all samples, i.e., the data is the same for the whole sequence.
- `file_extensions` (class method): returns the list of valid file extensions.
- `make_new` (class method): creates a new item of the same type.

Please note that the property `is_shared` is set by the *creator* of the Item and **no check** is performed by pipelime. For example, when reading an underfolder dataset, the items under the root folder are considered *shared* and added to each sample. However, nothing prevent you to create a shared Item and add it to a single sample. Though unusual, this trick may be useful to quickly add new root items to an underfolder dataset:

```python
from pipelime.sequences import SamplesSequence, Sample
from pipelime.items import YamlMetadataItem

# load a sequence
seq = SamplesSequence.from_underfolder("datasets/mini_mnist")

# create a new shared item
item = YamlMetadataItem({"foo": "bar"}, shared=True)

# add the item to a new sequence
new_seq = SamplesSequence.from_list([Sample({"meta_item": item})])

# concatenate and write the two sequences
seq = seq.cat(new_seq).to_underfolder("datasets/mini_mnist_with_meta")
seq.run()
```

In the example above, we added a new sample with a single shared item at the end of a sequence. Therefore, when writing the sequence to disk the item is saved in the root folder of the dataset.

## Serialization Modes

When an item is saved to disk, pipelime uses the `serialization_mode` property to determine how to save the data. The following modes are available:
1. `CREATE_NEW_FILE`: a new file is created by encoding the raw data. NB: if the source is a file, such file is loaded, decoded and then the data is encoded and save to disk again.
2. `DEEP_COPY`: if the source is file, such file is deep copied; otherwise, `CREATE_NEW_FILE` applies.
3. `SYM_LINK`: if the source is a file, a [symbolic link](https://en.wikipedia.org/wiki/Symbolic_link) is created; otherwise, `DEEP_COPY` applies.
4. `HARD_LINK`: is the source is a file, a [hard link](https://en.wikipedia.org/wiki/Hard_link) is created; otherwise, `DEEP_COPY` applies.
5. `REMOTE_FILE`: if the data comes from a [remote data lake](../advanced/remotes.md), a special `.remote` file is saved with a reference to the data lake; otherwise, `HARD_LINK` applies.

If you are not familiar with symbolic and hard links, these are the main differences:
- hard links are the usual "data" pointer you find in your filesystem, while symbolic links are "pointers" to other files;
- hard links do not link paths on different volumes or file systems, whereas symbolic links do;
- when you delete a hard link, you are actually reducing a reference counter, so the underline data is deleted when the last hard link is removed;
- on the other hand, a symbolic link and its target file do not talk to each other, so deleting a symbolic link does not affect the pointed file, while deleting the file makes the link invalid;
- therefore, hard links always refer to an existing file, whereas symbolic links may be broken.

Clearly, hard links are the most efficient way to manipulate a huge datasets, since when saving a sequence to disk, only the new data are actually written, while the rest is just *hard*-linked, which usually lightning-fast. Hence, the default serialization mode is set to `REMOTE_FILE`, which fall backs to `HARD_LINK` when a remote data lake is not given.

In order to change the default serialization mode, you have several options:
- set the `serialization_mode` property on each item;
- set a `key_serialization_mode` on the writer;
- use a built-in context manager or decorator to set the serialization mode for a specific block of code.

The first approach is usually unsuitable, while the second one is easy to implement: just pass a `<key>: <mode>` dictionary to `to_underfolder`:

```python
from pipelime.sequences import SamplesSequence

# saving "image" items as new files, while hard-linking the others
seq = SamplesSequence.from_underfolder("datasets/mini_mnist")
seq = seq.to_underfolder("datasets/mini_mnist_copy", key_serialization_mode={"image": "CREATE_NEW_FILE"})
seq.run()
```

The last option is the most flexible, since you can use `pli.item_serialization_mode` and `pli.item_disabled_serialization_modes` as context managers or function decorators to temporarily change the serialization mode for all items or specific item classes. For example:

```python
from pipelime.sequences import SamplesSequence
import pipelime.items as pli

seq = SamplesSequence.from_underfolder("datasets/mini_mnist")

# save all items as deep copies
with pli.item_serialization_mode("DEEP_COPY"):
    seq.to_underfolder("datasets/mini_mnist_copy").run()

# save only image items as deep copies, while keeping the default mode for the rest
with pli.item_serialization_mode("DEEP_COPY", pli.ImageItem):
    seq.to_underfolder("datasets/mini_mnist_imgcopy").run()

# save images as deep copies, metadatas as symbolic links and everything else as hard links
with pli.item_serialization_mode("DEEP_COPY", pli.ImageItem):
    with pli.item_serialization_mode("SYM_LINK", pli.MetadataItem):
        seq.to_underfolder("datasets/mini_mnist_imgcopy_metalnk").run()

# disabling HARD_LINK and DEEP_COPY for all items, they will be loaded and saved as new files
with pli.item_disabled_serialization_modes(["HARD_LINK", "DEEP_COPY"]):
    seq.to_underfolder("datasets/mini_mnist_allnew").run()
```

```{admonition} TIP
:class: tip

When you set the serialization mode on a base class, such as `NumpyItem`, it will affect
derive classes too. Indeed, pipelime goes through all base classes of an item and chooses
the *lowest* mode according to this order: `REMOTE_FILE` > `HARD_LINK` > `SYM_LINK` > `DEEP_COPY` > `CREATE_NEW_FILE`.
```

```{admonition} NOTE
:class: note

When serializing an item, either to disk or to a remote data lake, the content does not change,
so it is safely added a new *source* to the same item object. Then, subsequent serializations
might take advantage of that, e.g., by hard-linking the existing file instead of creating
again a new one.
```

## Data Caching

The first time you get the data from an item, such raw data is internally saved.
This way, subsequent calls to `__call__` will return the cached data, instead of loading
again from disk or from a remote data lake. Moreover, even if the samples are processed
through a long sequence of stages and pipes, as long as an item does not change, i.e.,
it is shallow copied, its cached data is always returned.

You can alter this behavior in the following ways:
- by setting the `cache_data` on each item
- by using the built-in context managers and decorators `data_cache` and `no_data_cache`
- by adding `data_cache` and `no_data_cache` steps into the pipeline

Again, the first option is usually awkward, though it may have some use cases.
The second approach is useful when you want to disable the caching for a specific block of code,
e.g., in a custom stage you know it will load a lot of data:

```python
from pipelime.sequences import Sample
from pipelime.stages import SampleStage
from pipelime.items import no_data_cache


class HeavyStage(SampleStage):
    """This stage loads a lot of data."""

    @no_data_cache()
    def __call__(self, x: Sample) -> Sample:
        ...
        return x
```

Finally, using pipeline steps is the most flexible way to control the caching behavior.
Each time you add a `no_/data_cache`, it applies to all the previous steps:

```python
from pipelime.sequences import SamplesSequence
from pipelime.stages import StageLambda

def get_data(x, k):
    """A simple function to force data loading."""
    _ = x[k]()
    return x

seq = SamplesSequence.from_underfolder("datasets/mini_mnist")

# force loading "label", which is a TxtNumpyItem
seq = seq.map(StageLambda(lambda x: get_data(x, "label")))

# enable caching of TxtNumpyItem (you can use `pipeline.items.TxtNumpyItem` as well)
seq = seq.data_cache("TxtNumpyItem")

# force loading "metadata", which is a JsonMetadataItem
seq = seq.map(StageLambda(lambda x: get_data(x, "metadata")))

# disable caching for all item types
seq = seq.no_data_cache()

print(seq[0])  # you should see a value for "label", but not for "metadata"
```

```{admonition} TIP
:class: tip

The `cache_data` attribute always takes precedence over the global configuration, which
is taken into account only if `cache_data` is `None`. In such cases, the cache settings
for all base classes are considered and the first one not set to `None` is applied.

Initially, data caching is set to `None` both on the item objects and in the global configuration,
so the default behavior is to cache the data.
```

```{admonition} WARNING
:class: warning
Calling `no_/data_cache` with no arguments will disable/enable the caching
for **all** item types, so, for example, the following pipeline would cache no data:

```python
seq = SamplesSequence.from_underfolder("datasets/mini_mnist")
seq = seq.map(StageLambda(lambda x: get_data(x, "label")))  # "label" is a TxtNumpyItem
seq = seq.data_cache("NumpyItem")
seq = seq.map(StageLambda(lambda x: get_data(x, "metadata")))
seq = seq.no_data_cache()
```

## Custom Items

To support your custom data format you can create a new item class and implement a few
core methods. Though your base class must be `pipelime.items.Item`, it is advisable to
derive from existing item classes if they share the same data type or format.
For instance, all image items (`PngImageItem`, `BmpImageItem` etc.) derive from
`ImageItem` which is a child of `NumpyItem` which derive from `Item`.

To write your own item you must provide the implementation of the following class methods:
- **file_extensions**: returning a list of recognized file extension
- **decode**: returning the data read from an input stream
- **encode**: writing the data into a given output stream
- **validate** *(optional)*: parsing and validating general raw data
- **pl_pretty_data** *(optional)*: returning a representation of the data ready to be printed on a [rich](https://rich.readthedocs.io/en/stable/index.html)-like console

A simple demonstration is given below:

```python
import typing as t
from scipy.io import wavfile
import numpy as np

from pipelime.items import Item

class WavItem(Item[np.ndarray]):
    _sample_rate: int

    def __init__(self, *args, sample_rate: int = 44100, **kwargs):
        super().__init__(*args, **kwargs)
        self._sample_rate = sample_rate

    @property
    def sample_rate(self) -> int:
        return self._sample_rate

    @sample_rate.setter
    def sample_rate(self, value: int):
        self._sample_rate = value

    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".wav",)

    @classmethod
    def decode(cls, fp: t.BinaryIO) -> np.ndarray:
        sample_rate, data = wavfile.read(fp)
        self.sample_rate = sample_rate
        return data

    @classmethod
    def encode(cls, value: np.ndarray, fp: t.BinaryIO):
        wavfile.write(fp, self.sample_rate, value)

    @classmethod
    def validate(cls, raw_data: t.Any) -> np.ndarray:
        data = np.array(raw_data)
        if data.dtype == np.float32:
            assert data.min() >= -1 and data.max() <= 1, "Float32 WAV must be in [-1,1]"
        else:
            assert (
                data.dtype in (np.int32, np.int16, np.uint8),
                "WAV data type must be one of float32, int32, int16 or uint8",
            )
        return data
```

This new `WavItem` loads a WAV audio file and exposes the sample rate as property.
Being the data a numpy array, we can improve the integration with the built-in types
by deriving from `NumpyItem` instead of simply `Item`:

```python
from pipelime.items import NumpyItem

class WavItem(NumpyItem):
    ...

    @classmethod
    def validate(cls, raw_data: t.Any) -> np.ndarray:
        data = super().validate(raw_data)

        if data.dtype == np.float32:
            assert data.min() >= -1 and data.max() <= 1, "Float32 WAV must be in [-1,1]"
        else:
            assert (
                data.dtype in (np.int32, np.int16, np.uint8),
                "WAV data type must be one of float32, int32, int16 or uint8",
            )
        return data
```
