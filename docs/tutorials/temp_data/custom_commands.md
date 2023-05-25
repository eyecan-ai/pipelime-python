# Temporary Folders In Your Scripts

When you are [extending Pipelime](../../operations/intro.md), eg, by creating new stages or commands, you may need to create temporary folders to store intermediate data.
In such cases, you can use the class `PipelimeTemporaryDirectory`
from the `pipelime.choixe.utils.io` module as a drop-in replacement for the `tempfile.TemporaryDirectory` class. For example:

```python
from pipelime.choixe.utils.io import PipelimeTemporaryDirectory

with PipelimeTemporaryDirectory() as tmp_dir:
    ... # do something with tmp_dir
# tmp_dir is deleted here

tmppath = PipelimeTemporaryDirectory()
tmp_dir = tmppath.name
... # do something with tmp_dir
tmppath.cleanup()  # tmp_dir is deleted here

# alternatively, tmp_dir is deleted when tmppath is garbage collected
del tmppath
```

Using `PipelimeTemporaryDirectory` instead of `tempfile.TemporaryDirectory` make sure that all the temporary folders created in the same session are placed within the same parent folder in the user temporary directory. Such parent folder is automatically named as `pipelime-of-<username>-<session-id>`, so that it can be easily identified.

## Temporary Folders Persistency

By default, the temporary folders created by Pipelime are deleted after the execution of a command or when the `PipelimeTemporaryDirectory` object is garbage collected. However, you may want to keep them for debugging purposes. To this end, when running a command just add the `-t` flag, eg:

```bash
$ pipelime split +i input_data +s 0.5,train +s '0.3,$tmp()/null' +s 0.2,test -t
```

or directly use the `PipelimeTmp` class instead of `PipelimeTemporaryDirectory`:

```python
tmp_dir = PipelimeTmp.make_subdir('my_tmp_dir')
... # do something with tmp_dir

# tmp_dir is never deleted
```

Then, go to the next [step](./tmp_command.md) to show and delete the temporary data created by Pipelime.
