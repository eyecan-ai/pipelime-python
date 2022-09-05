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
5. `REMOTE_FILE`: if the data comes from a remote data lake, a special `.remote` file is saved with a reference to the data lake; otherwise, `HARD_LINK` applies.

If you are not familiar with symbolic and hard links, these are the main differences:
- hard links are the usual "data" pointer you find in your filesystem, while symbolic links are "pointers" to other files;
- hard links do not link paths on different volumes or file systems, whereas symbolic links do;
- when you delete a hard link, you are actually reducing a reference counter, so the underline data is deleted when the last hard link is removed;
- on the other hand, a symbolic link and its target file do not talk to each other, so deleting a symbolic link does not affect the pointed file, while deleting the file makes the link invalid;
- therefore, hard links always refer to an existing file, whereas symbolic links may be broken.

Clearly, hard links are the most efficient way to manipulate a huge datasets, since when saving a sequence to disk, only the new data is actually written, while the rest is just *hard*-linked. Hence, the default serialization mode is set to `REMOTE_FILE`, which fall backs to `HARD_LINK` when a remote data lake is not given.

In order to change the default serialization mode, you have several options:
- set the `serialization_mode` property on each item;
- set a `key_serialization_mode` on the writer;
- use a context manager or a decorator to set the serialization mode for a specific block of code.

The first approach is usually unsuitable, while the second one is easy to implement: just pass a `<key>: <mode>` dictionary to `to_underfolder`. The last one is the most flexible, since you can use `pli.item_serialization_mode` and `pli.item_disabled_serialization_modes` both as context managers or function decorators to temporarily change the serialization mode for all items or specific classes. For example:

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

## Data Caching

## Custom Items
