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

## Writing Samples To A New Underfolder
