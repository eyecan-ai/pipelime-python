# Data Streaming

Sometimes you need to decouple data reading and writing, because, e.g., not all the samples should be written or you need to externally process the data.
For example, you may have a GUI to browse a dataset which allows the user to modify the samples.
In this case you need a reader to load the data, but you want to call the writer only when the user saves the changes.
Also, you may want to overwrite the original samples with the modified ones and immediately show the updated samples in your GUI.

All of this can be achieved with the base `SamplesSequence` class, but pipelime provides also the `DataStream` class to automate the most common use cases.

## General Definition

The `DataStream` class lives in `pipelime.sequences` and it is a sequence of samples, though it is **not** a subclass of `SamplesSequence`.
To instiantiate a `DataStream` object you need to provide an optional `input_sequence`, i.e., a `SamplesSequence`, and an `output_pipe`, i.e., a list of operations (cfr. [`build_pipe`](../get_started/operations.md#deserialization)):

```python
from pipelime.sequences import SamplesSequence, DataStream

reader = SamplesSequence.from_underfolder("datasets/mini_mnist")
outpipe = [
    "to_underfolder": {
        "folder": "output",
        "zfill": 6,
        "exists_ok": False,
    }
]
stream = DataStream(input_sequence=reader, output_pipe=outpipe)
```

Then you can read and write samples:

```python
sample = stream[0]
sample, new_idx = do_something(sample)
stream.set_output(new_idx, sample)
```

## Dynamically Updating An Existing Underfolder

If the reader and the writer share the same folder, which may or may not exist, you can create the `DataStream` by calling the `read_write_underfolder` class method:

```python
from pipelime.sequences import DataStream

stream = DataStream.read_write_underfolder(
    "datasets/mini_mnist", must_exist=True, zfill=None
)
```

Beside the path to the folder, the optional parameters include:
- `must_exist`: if True, the folder must exist, otherwise an error is raised
- `zfill`: if not None, must be an integer specifyng the number of digits to use for the sample index, otherwise the padding is taken from the length of the original sequence

Note that the underlying reader has the `watch` option active, so that every time you ask for a sample, you get the last written sample.

## Writing Samples To A New Underfolder

A very common use case is to gather data outputted by some algorithm and save it to a new underfolder dataset. Therefore, the target folder *must not* exist.
The `create_new_underfolder` class method setups the `DataStream` for this use case and raise an error if the folder already exists:

```python
from pipelime.sequences import DataStream

stream = DataStream.create_new_underfolder(
    "datasets/mini_mnist", zfill=6
)
```

Note that in this case the `zfill` must be explicitly specified, because the writer does not have a reader to infer it from.
