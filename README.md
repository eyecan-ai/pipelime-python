![](https://github.com/eyecan-ai/pipelime-python/blob/main/docs/pipelime_logo.svg?sanitize=true)

# 🍋 Pipelime

*If life gives you lemons, use Pipelime.*

[![Documentation Status](https://readthedocs.org/projects/pipelime-python/badge/?version=latest)](https://pipelime-python.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/pipelime-python.svg)](https://badge.fury.io/py/pipelime-python)

Welcome to **Pipelime**, a swiss army knife for data processing!

Pipelime is a full-fledge **framework** for **data science**: read your datasets,
manipulate them, write back to disk or upload to a remote data lake.
Then build up your **dataflow** with Piper and manage the configuration with Choixe.
Finally, **embed** your custom commands into the Pipelime workspace, to act both as dataflow nodes and advanced command line interface.

Maybe too much for you? No worries, Pipelime is **modular** and you can just take out what you need:
- **data processing scripts**: use the powerful `SamplesSequence` and create your own data processing pipelines, with a simple and intuitive API. Parallelization works out-of-the-box and, moreover, you can easily serialize your pipelines to yaml/json. Integrations with popular frameworks, e.g., [pytorch](https://pytorch.org/), are also provided.
- **easy dataflow**: Piper can manage and execute directed acyclic graphs (DAGs), giving back feedback on the progress through sockets or custom callbacks.
- **configuration management**: Choixe is a simple and intuitive mini scripting language designed to ease the creation of configuration files with the help of variables, symbol importing, for loops, switch statements, parameter sweeps and more.
- **command line interface**: Pipelime can remove all the boilerplate code needed to create a beautiful CLI for you scripts and packages. You focus on *what matters* and we provide input parsing, advanced interfaces for complex arguments, automatic help generation, configuration management. Also, any pipelime command can be used as a node in a dataflow for free!
- **pydantic tools**: most of the classes in Pipelime derive from [`pydantic.BaseModel`](https://pydantic-docs.helpmanual.io/), so we have built some useful tools to, e.g., inspect their structure, auto-generate human-friendly documentation and more (including a wizard to help you writing input data to [deserialize](https://pydantic-docs.helpmanual.io/usage/models/#helper-functions) any pydantic model).

---

## Installation

Install Pipelime using pip:

```
pip install pipelime-python
```

To be able to *draw* the dataflow graphs, you need the `draw` variant:

```
pip install pipelime-python[draw]
```

> **Warning**
>
> The `draw` variant needs `Graphviz` <https://www.graphviz.org/> installed on your system
> On Linux Ubuntu/Debian, you can install it with:
>
> ```
> sudo apt-get install graphviz graphviz-dev
> ```
>
> Alternatively you can use `conda`
>
> ```
> conda install --channel conda-forge pygraphviz
> ```
>
> Please see the full options at https://github.com/pygraphviz/pygraphviz/blob/main/INSTALL.txt

## Basic Usage

### Underfolder Format

The **Underfolder** format is the preferred pipelime dataset formats, i.e., a flexible way to
model and store a generic dataset through **filesystem**.

![](https://github.com/eyecan-ai/pipelime-python/blob/main/docs/images/underfolder.png?raw=true)

An Underfolder **dataset** is a collection of samples. A **sample** is a collection of items.
An **item** is a unitary block of data, i.e., a multi-channel image, a python object,
a dictionary and more.
Any valid underfolder dataset must contain a subfolder named `data` with samples
and items. Also, *global shared* items can be stored in the root folder.

Items are named using the following naming convention:

![](https://github.com/eyecan-ai/pipelime-python/blob/main/docs/images/naming.png?raw=true)

Where:

* `$ID` is the sample index, must be a unique integer for each sample.
* `ITEM` is the item name.
* `EXT` is the item extension.

We currently support many common file formats and others can be added by users:

  * `.png`, `.jpeg/.jpg/.jfif/.jpe`, `.bmp` for images
  * `.tiff/.tif` for multi-page images and multi-dimensional numpy arrays
  * `.yaml/.yml`, `.json` and `.toml/.tml` for metadata
  * `.txt` for numpy 2D matrix notation
  * `.npy` for general numpy arrays
  * `.pkl/.pickle` for pickable python objects
  * `.bin` for generic binary data

Root files follow the same convention but they lack the sample identifier part, i.e., `$ITEM.$EXT`

### Reading an Underfolder Dataset

Pipelime provides an intuitive interface to read, manipulate and write Underfolder Datasets.
No complex signatures, weird object iterators, or boilerplate code, you just need a `SamplesSequence`:

```python
    from pipelime.sequences import SamplesSequence

    # Read an underfolder dataset with a single line of code
    dataset = SamplesSequence.from_underfolder('tests/sample_data/datasets/underfolder_minimnist')

    # A dataset behaves like a Sequence
    print(len(dataset))             # the number of samples
    sample = dataset[4]             # get the fifth sample

    # A sample is a mapping
    print(len(sample))              # the number of items
    print(set(sample.keys()))       # the items' keys

    # An item is an object wrapping the actual data
    image_item = sample["image"]    # get the "image" item from the sample
    print(type(image_item))         # <class 'pipelime.items.image_item.PngImageItem'>
    image = image_item()            # actually loads the data from disk (may have been on the cloud as well)
    print(type(image))              # <class 'numpy.ndarray'>
```

### Writing an Underfolder Dataset

You can **write** a dataset by calling the associated operation:

```python
    # Attach a "write" operation to the dataset
    dataset = dataset.to_underfolder('/tmp/my_output_dataset')

    # Now run over all the samples
    dataset.run()

    # You can easily spawn multiple processes if needed
    dataset.run(num_workers=4)
```
