# Sequences

Sample Sequences are at the core of all pipelime features. When writing custom operators like stages, pipes and commands, you will often need to access individual samples and items and manipulate them. 

In this tutorial we will se how to interact with sample sequences and samples, their basic methods and their behaviour. Keep in mind that, yes, you could implement a data pipeline by just accessing and modifying individual samples and items, but that is strongly discouraged: you would either have to write a lot of boilerplate code, or miss out on a lot of features that pipelime provides automatically if you choose to follow the general framework of stages/pipes/commands.

## First step

First you should import the `pipelime.sequences` module, we suggest to alias it as `pls`. Inside it you will find core pipelime classes like `SampleSequence` and `Sample` and some useful functions to manipulate them.

We suggest to also import `pipelime.items` aliased as `pli`. Here you will find all built-in item specializations and some useful lower-level utilities. 

```python
import pipelime.sequences as pls
import pipelime.items as pli
```

## Reading

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

So far, **no** actual data loading has been performed. 
