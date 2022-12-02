# Samples And Sequences

Sample Sequences are at the core of all pipelime features. When writing custom operators like stages, pipes and commands, you will often need to access individual samples and items and manipulate them.

In this tutorial we will se how to interact with sample sequences and samples, their basic methods and their behaviour. Keep in mind that you could implement a data pipeline by just accessing and modifying individual samples and items, but that is strongly discouraged: you would either have to write a lot of boilerplate code, or miss out on a lot of features that pipelime provides automatically if you choose to follow the general framework of stages/pipes/commands.

## Relevant Modules

`SamplesSequence` and `Sample` are defined in the `pipelime.sequences` subpackage.
If you want to import everything, we suggest to alias it as `pls`.
We suggest to also import `pipelime.items` aliased as `pli`.
Here you will find all built-in item specializations and some useful lower-level utilities.

```python
from pipelime.sequences import SamplesSequence
import pipelime.items as pli
```

## Reading Data

In the example project we provide a demonstration dataset named "mini_mnist", which contains 20 samples of the MNIST dataset, randomly colorized, with some metadata.

Let's create a sample sequence pointing to that dataset:

```python
seq = SamplesSequence.from_underfolder("datasets/mini_mnist")
```

As you may notice, the creation of a sample sequence is almost instantaneous. In fact, the sample sequence object is lazy, and does not read anything from disk until it is explicitly requested.

The `seq` object works like a python `Sequence`, you can get its length:

```python
print(len(seq))
```

```bash
>>>
20
```

Or you can get individual samples by subscripting it:

```python
sample_7 = seq[7]
sample_19 = seq[-1]
```

You can also **slice** it, obtaining a view of a subset of the samples:

```python
subseq = seq[4:10:2]
print(len(subseq))
```

```bash
>>>
3
```

Here, `subseq` is another sample sequence with only samples 4, 6 and 8.

We can also take a peek inside individual samples, which behave like python `Mapping` objects. Let's take a look at `sample_7`:

```python
print(len(sample_7))
```

```bash
>>>
8
```

```
print(list(sample_7.keys()))
```

```bash
>>>
['common', 'numbers', 'label', 'maskinv', 'metadata', 'points', 'mask', 'image']
```

As you may notice, the keys of `sample_7` match the names of the files inside `datasets/mini_mnist/data`, with the only exception of `common` and `numbers`, which are "shared items", i.e. items that are shared across all samples and stored in files outside the `data` subfolder to avoid unnecessary redundancy.

We can access individual items by subscripting the sample object:

```python
image_item = sample_7["image"]
print(image_item)
```

```bash
>>>
JpegImageItem:
  data: None
  sources:
      - datasets/mini_mnist/data/000007_image.jpg
  remotes:
  shared: False
  cache: True
  serialization: None
```

Note that you can easily get a *rich* printing as well:

```python
from pipelime.cli import pl_print

# pl_print can print anything in pipelime: items, samples, sequences, commands, stages...
pl_print(sample_7["image"])
```

As you can see, the item is a `JpegImageItem`, referencing to a file named `000007_image.jpg`, it is not shared (as `common` and `numbers` are), and it has caching enabled, that is, the item will be loaded from disk only once and then cached in memory.

So far, **no** data loading has actually been performed.
To do so, we need to explicitly tell pipelime to get the data, by **calling** the item:

```python
image = image_item()
print(type(image), image.shape)
```

```bash
>>>
<class 'numpy.ndarray'> (28, 28, 3)
```

After the data has been loaded, it gets cached inside the item object: consecutive calls to `image_item` will simply return the cached data. If you wish to disable this behavior, you have two options:

1. Manually set the `cache_data` to `False` for a specific item. This has to be done before any data loading.

```python
image_item.cache_data = False
```

2. Use the context manager to disable the caching globally. You can also choose to disable it for a specific type of item.

```python
with pli.no_data_cache():
    image = image_item()
```

Generally, caching data can drastically speedup a pipeline in which you need to access the same data multiple times, but with the major drawback of increasing the memory usage.
However, you should also consider **when** data caching happens: if you are amidst of a complex processing, the item object you are loading may be a shallow copy of the original one, so the cached data will be lost when the item is destroyed.

## Modifying Data

In this section, we will write our first and very simple data pipeline. Please keep in mind that **there are far better ways to do this**, the example here is simply to make you familiar with the core pipelime functionalities.

Pipelime objects should be treated as immutable: i.e. you should not directly modify them, but only create altered copies of them. By *modifying* a sequence we really mean to create a new sequence, new samples and new items that contain altered versions of the original data, without never modifying anything in-place: the old sequence still remains untouched. Be aware, however, that pipelime usually shares any data that is not modified, so the memory footprint of chained operations is usually very low.

Why did we just say that pipelime objects *should be treated as* immutable and not that they simply *are* immutable? Well, this is python and there is no actual way to prevent you from, let's say, modifying a numpy array within an item inplace and pretend nothing has happened. We can simply consider this a **bad practice**, and ask you to avoid it, like you would avoid accessing a field that starts with `_`.

Let's modify the "mini_mnist" dataset by:
1. Keeping only the samples with even index.
2. Inverting the color of the images.
3. Adding a new item called `color` with the average image color.
4. Deleting the `maskinv` item.

We assume you already have a sequence from the previous example. If so, start by slicing the sequence to keep only the even samples:

```python
# Keep only the even samples
even_samples = seq[::2]
```

Then, we need to modify the remaining samples individually, thus we iterate over the `even_samples` in a for loop. Before doing that, since the `SamplesSequence` is an immutable object, we need to initialize a new list that will contain the new samples.

```python
# Initialize an empty list of samples
new_samples = []

# Iterate on the sequence
for sample in even_samples:
    ...
```

Let's then procede to invert the image item:

```python
    # Get the image item
    image: np.ndarray = sample["image"]()  # type: ignore

    # Invert the colors
    invimage = 255 - image

    # Replace the value of "image" with the inverted image
    sample = sample.set_value("image", invimage)
```

First, we explicitly avoided in-place changes to the image array by computing the inverse as `invimage = 255 - image`.
Then, to replace the previous image with the inverted one we used the method `set_value`. This method, under the hood, follows these steps:
1. Retrieves the `image` item from the sample.
2. Creates a new item of the same type, but with the new data.
3. Creates a copy of the sample with the new item and returns it.

It is important to note that what we get from `sample.set_value` is actually a **new sample** where all the item objects are shallow copied from the original sample, except for the `image` item, which is a new object of the same type. Therefore, simply calling the `set_value` method alone is completely useless, since you need to store the returned object in a variable. For simplicity, we save the new sample object in the same variable named `sample`, but they are two completely unrelated objects.

We proceed by adding a new item with the image average color:

```python
    # Get the average image color
    avg_color = np.mean(invimage, axis=(0, 1))

    # Create a numpy item with the average color and add it to the sample
    avg_color_item = pli.NpyNumpyItem(avg_color)
    sample = sample.set_item("color", avg_color_item)
```

In this case, we don't have a previously existing item whose value we need to modify, so, we need to create a new item from scratch. The `pli` module contains all sorts of built-in item types that you can use, here we choose `NpyNumpyItem`, i.e., a generic item storing numpy arrays in the npy format. To create it, we simply pass the data to the item constructor.
Item `__init__` method usually tries to convert and validate whatever you pass to it, so, for example, a `NpyNumpyItem` can accept anything you would pass to `numpy.array`.

To add the item to the sample, we do something similar to what we did in the previous step, but in this case, we have to call the `set_item` method, which returns a copy of the sample with the new item.

Going deeper, to add items you have the following options:
- `set_item`: when you have a new item already created, and you simply wish to set it as is. You control every aspect of the item you set, but the item creation is left to you: one more statement to your code.
- `set_value`: when you want to replace the value (the content) of an existing item, without any knowledge of what type of item it is. You have no control over the item creation, and you want to leave that to pipelime.
- `set_value_as`: when you want to set/replace a value with the same item type of another (possibly unrelated) item. Suppose you want to add a binary mask in a sample that already contains a png image item, and you wish that the mask item has the same type - but different content - as the image, then you can use `set_value_as`.

Back to our example, we then procede to remove the `maskinv` item.

```python
    # Delete the maskinv item
    sample = sample.remove_keys("maskinv")

    # After the sample has been modified, add it to the sequence
    new_samples.append(sample)
```

To modify keys, the following methods are available:
- `remove_keys`: removes one or more keys from the sample.
- `extract_keys`: removes all keys except the ones specified.
- `rename_key`: renames a key, keeping the same item.
- `duplicate_key`: duplicates a key, shallow-copying the item.

Finally, we then need to transform the list of new samples into a real `SamplesSequence`, by doing:

```python
new_seq = SamplesSequence.from_list(new_samples)
```

To wrap-up:

```python
from pipelime.sequences import SamplesSequence
import pipelime.items as pli

seq = SamplesSequence.from_underfolder("datasets/mini_mnist")

new_samples = []
for sample in even_samples:
    # Get the image item
    image: np.ndarray = sample["image"]()  # type: ignore

    # Invert the colors
    invimage = 255 - image

    # Replace the value of "image" with the inverted image
    sample = sample.set_value("image", invimage)

    # Get the average image color
    avg_color = np.mean(invimage, axis=(0, 1))

    # Create a numpy item with the average color and add it to the sample
    avg_color_item = pli.NpyNumpyItem(avg_color)
    sample = sample.set_item("color", avg_color_item)

    # Delete the maskinv item
    sample = sample.remove_keys("maskinv")

    # After the sample has been modified, add it to the sequence
    new_samples.append(sample)

# Create a new sequence from the list of samples
new_seq = SamplesSequence.from_list(new_samples)
```

## Writing Data

After you modify a sample sequence, you might want to write it back to disk.
This is very simple, since you just need to attach a writer to the sequence, and then iterate over it (or let pipelime do it for you by calling `run`):

```python
# Save the new sequence to an underfolder
new_seq = new_seq.to_underfolder("datasets/mini_mnist_inv")
new_seq.run()
```

Notice two things:
- `to_underfolder` does not write anything on its own. In fact, it just chains a pipe that writes a sample to disk every time you *get* it.
- in order to actually write it to disk we use the `run` method, that simply iterates over the sequence, possibly using multiple processes.

We could replace the `run` call with something like:

```python
for x in new_seq:
    pass
```

or

```python
[_ for x in new_seq]
```

but why would you do that?

## Pipe Caching

When you build a complex pipeline and repeatedly extract samples from it, you might want to cache the results of some steps, so that you don't have to recompute them every time. To this end, you can use the `cache` method, which takes a `cache_dir` argument and returns a new sequence that *pickles* the samples of the previous one the first time they are extracted. Then, only the pickle will be loaded the next time.

```python
seq = seq.cache("cache_dir")
```

Notice that if no `cache_dir` is specified, it will use a temporary directory.
This pipe is designed to be used with multiple processes, so it safe to share the same cache folder while accessing the same samples.

## Advanced API

We have seen so far a bunch of methods to modify a `Sample`:
- `set_item`, `set_value`, `set_value_as`: to add/replace items.
- `remove_keys`, `extract_keys`, `rename_key`, `duplicate_key`: to modify keys.

Beside the mehods above, `Sample` includes an advanced API to access and process the data.

Serialization:
- `to_schema`: returns a `{<key>: <item-type>}` dictionary.
- `to_dict`: returns a `{<key>: <item-value>}` dictionary.

Copy:
- `shallow_copy`: returns a new sample where the internal data dictionary is shallowed copied.
- `deep_copy`: returns a new sample where the all the data is deep copied.

Advanced access:
- `deep_get`: returns an internal value through a key path similar to [`pydash.get`](https://pydash.readthedocs.io/en/latest/api.html#pydash.objects.get); of course, the item's data should be a suitable data structure, such as a mapping or a sequence.
- `deep_set`: sets an internal value through a key path similar to [`pydash.set_`](https://pydash.readthedocs.io/en/latest/api.html#pydash.objects.set_); as usual, the item is cloned before setting the new value and a new sample is returned.
- `direct_access`: returns a special object with a mapping-like interface that directly returns the item values when accessing a key, instead of the item objects themselves.

Querying:
- `match`: applies a [`dictquery`](https://github.com/cyberlis/dictquery) query to the sample and returns the result.

Combining samples:
- `merge`, `update`, `zip`: returns a new sample with all the items of the current sample *updated* with the ones of the other sample, i.e., items with the same key are replaced with the ones of the other sample.

Operators:
- `__add__`: `s1 + s2` is equivalent to `s1.merge(s2)`
- `__sub__`: `s1 - s2` is equivalent to `s1.remove_keys(*s2.keys())`
- `__and__`: `s1 & s2` is equivalent to `s1.extract_keys(*s2.keys())`
- `__or__`: `s1 | s2` is equivalent to `s1.merge(s2)`
- `__xor__`: `s1 ^ s2` is equivalent to `(self | other) - (self & other)`


Likewise, `SamplesSequence` includes some advanced methods as well:
- `__add__`: you can concatenate two sequences with the `+` operator.
- `is_normalized`: check if all samples have the same keys.
- `to_pipe`: serialize the sequence to a list that can be saved to disk as yaml/json and later deserialized with `build_pipe`.
- `direct_access`: returns a new object with a sequence-like interface that directly returns `{<key>: <item-value>}` dictionaries when accessing a sample, instead of the sample objects themselves.
- `torch_dataset`: returns a new object derived from `torch.utils.data.Dataset` that can be used to load data into [PyTorch](https://pytorch.org/), e.g., through a `torch.utils.data.DataLoader`.
- `batch`: returns a zip-like object to get batches of samples.

As for the batching, here is an example:

```python
for batch in seq.batch(32):
    for sample in batch:
        print(sample)
```
