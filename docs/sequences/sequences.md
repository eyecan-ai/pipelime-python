# Sequences

Sample Sequences are at the core of all pipelime features. When writing custom operators like stages, pipes and commands, you will often need to access individual samples and items and manipulate them. 

In this tutorial we will se how to interact with sample sequences and samples, their basic methods and their behaviour. Keep in mind that, yes, you could implement a data pipeline by just accessing and modifying individual samples and items, but that is strongly discouraged: you would either have to write a lot of boilerplate code, or miss out on a lot of features that pipelime provides automatically if you choose to follow the general framework of stages/pipes/commands.

## Necessary Modules

First you should import the `pipelime.sequences` module, we suggest to alias it as `pls`. Inside it you will find core pipelime classes like `SampleSequence` and `Sample` and some useful functions to manipulate them.

We suggest to also import `pipelime.items` aliased as `pli`. Here you will find all built-in item specializations and some useful lower-level utilities. 

```python
import pipelime.sequences as pls
import pipelime.items as pli
```
 
## Reading Data

In the example project we provide a demonstration dataset named "mini_mnist", which contains 20 samples of the MNIST dataset, randomly colorized, with some metadata.

Let's create a sample sequence pointing to that dataset:

```python
seq = pls.SamplesSequence.from_underfolder("datasets/mini_mnist")
``` 

As you may notice, the creation of a sample sequence is almost instantaneous. In fact, the sample sequence object is lazy, and does not read anything from disk until it is explicitly requested.

The `seq` object works like a python `Sequence`, you can get its length:

```python
print(len(seq))
# >>> 20
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
# >>> 3
```

Here, `subseq` is another sample sequence with only samples 4, 6 and 8.

We can also take a peek inside individual samples, which behave like python `Mapping` objects. Let's take a look at `sample_7`:

```python
print(len(sample_7))
# >>> 8

print(list(sample_7.keys()))
# >>> ['common', 'numbers', 'label', 'maskinv', 'metadata', 'points', 'mask', 'image']
```

As you may notice, the keys of `sample_7` match the names of the files inside `datasets/mini_mnist/data`, with the only exception of "common" and "numbers", which are "shared items", i.e. items that are shared across all samples and stored in files outside the `data` subfolder to avoid unnecessary redundancy.

We can access individual items by subscripting the sample object:

```python
image_item = sample_7["image"]
print(image_item)
# >>> JpegImageItem:
#       data: None
#       sources:
#           - datasets/mini_mnist/data/000007_image.jpg
#       remotes:
#       shared: False
#       cache: True
#       serialization: None
``` 

As you can see, the item is a `JpegImageItem`, referencing to a file named `000007_image.jpg`, it is not shared (as "common" and "numbers" should be), and has caching enabled.

So far, **no** data loading has actually been performed, to do so, we need to explicitly tell pipelime to do so, by **calling** the item:

```python
image = image_item()
print(type(image), image.shape)
# >>> <class 'numpy.ndarray'> (28, 28, 3)
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

## Modifying Data

In this section, we will write our first and very simple data pipeline. Please keep in mind that **there are far better ways to do this**, the example here is simply to make you familiar with the core pipelime functionalities.

Pipelime objects should be treated as immutable: i.e. you should not directly modify them, but only create altered copies of them. By "modifying" a sequence we really mean to create a new sequence, new samples and new items that contain altered versions of the original data, without never modifying anything in-place: the old sequence still remains untouched.

Why did we just say that pipelime objects *should be treated as* immutable and not that they simply *are* immutable? Well, this is python and there is no actual way to prevent you from, let's say, modifying a numpy array within an item inplace and pretend nothing has happened. We can simply consider this a bad practice, and ask you to avoid it, like you would avoid accessing a field that starts with "_".

Let's modify the "mini mnist" dataset by:
1. Keeping only the samples with even index.
2. Inverting the color of the images.
3. Adding a new item called "color" with the average image color.
4. Deleting the "maskinv" item.

We assume you already have a sequence from the previous example. If so, start by slicing the sequence to keep only the even samples:

```python
# Keep only the even samples (POINT 1)
even_samples = seq[::2]
```

Then, we need to modify the remaining samples individually, thus we iterate over the `even_samples` in a for loop. Before doing that, since the `SampleSequence` is an immutable object, we need to initialize a new list that will contain the new samples. 

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

    # Replace the value of "image" with the inverted image (POINT 2)
    sample = sample.set_value("image", invimage)
```

First we explicitly avoided in-place changes to the image array, despite it being mutable.

Then, to replace the previous image with the inverted one we used the method `set_value`. This method, under the hood does the following:
1. Retrieves the "image" item from the sample.
2. Creates a copy of the item with the new data
3. Creates a copy of the sample with the new item and returns it.

Note that simply calling the `set_value` method alone is completely useless, you need to store the returned object in a variable. For simplicity we save the new sample object in the same variable named `sample`, but they are two completely unrelated objects.

We proceed then to adding a new item with the image average color:

```python
    # Get the average image color
    avg_color = np.mean(invimage, axis=(0, 1))

    # Create a numpy item with the average color and add it to the sample (POINT 3)
    avg_color_item = pli.NpyNumpyItem(avg_color)
    sample = sample.set_item("avg_color", avg_color_item)
```

In this case, we don't have a previously existing item whose value we need to modify, so, we need to create a new item from scratch. 

The `pli` module contains all sorts of built-in item types that you can use, here we choose `NpyNumpyItem` i.e. a generic item that stores numpy array in the npy format. To create it, we simply pass the data to the item constructor.

To add the item to the sample, we do something similar to what we did in the previous step, but in this case, we have to call the `set_item` method, which returns a copy of the sample with the new item.

To add items you have the following options:
- `set_item` - When you have a new item already created, and you simply wish to set it as is. You control every aspect of the item you set, but the item creation is left to you: one more statement to your code.
- `set_value` - When you want to replace the value (the content) of an existing item, without any knowledge of what type of item it is. You have no control over the item creation, and you want to leave that to pipelime.
- `set_value_as` - When you want to set/replace a value with the same item type of another (possibly unrelated) item. Suppose you want to add a binary mask in a sample that already contains a png image item, and you wish that the mask item has the same settings - but different content - as the image, then you can use `set_value_as`. Basically create a new item as the copy of another one, but with different value.
  
Back to our example, we then procede to remove the "maskinv" item.

```python
    # Delete the maskinv item (POINT 4)
    sample = sample.remove_keys("maskinv")

    # After the sample has been modified, add it to the sequence
    new_samples.append(sample)
```

We then need to transform the list of new samples into a real `SamplesSequence`, by doing: 

```python
new_seq = pls.SamplesSequence.from_list(new_samples)
```

## Writing Data

After you modify a sample sequence, you might want to write it back to disk. Doing this is very simple, you just need to transform the sequence into a writer, and then iterate over it:


```python
# Save the new sequence to an underfolder
new_seq = new_seq.to_underfolder(this_folder / "datasets/mini_mnist_inv")
new_seq.run()
```

Notice two things:
- `to_underfolder` does not write anything on its own. Really it just creates another sequence that is written on disk upon iteration.
- in order to actually write it to disk we use the `run` method, that simply iterates over the sequence.

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