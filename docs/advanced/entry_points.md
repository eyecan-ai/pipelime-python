# Project Entry Points

Using pipelime, you can skip all the boilerplate code needed to create a command line interface for your own python package.
You just have to wrap your logic within `PipelimeCommand`s, `SampleStage`s, pipes and generators,
then pipelime will take care of the rest.

For example, consider this package structure:

```
my_project
├─ __init__.py
├─ commands
│   ├─ __init__.py
│   ├─ cmd0.py
│   └─ cmd1.py
├─ stages
│   ├─ __init__.py
│   ├─ stg0.py
│   └─ stg1.py
└─ sequences
    ├─ __init__.py
    ├─ gen.py
    └─ pipe.py
```

where `commands` contains the commands, `stages` the stages, and `sequences` the pipes and generators.
To let pipelime discover your code, you should use the `-m` flag for each module, so the easiest way is to fill
`my_project/__init__.py` with the following imports:

```python
from my_project.commands.cmd0 import CmdZero
from my_project.commands.cmd1 import CmdOne

# these might be moved to `my_project/stages/__init__.py` and imported only if needed
from my_project.stages.stg0 import StgZero
from my_project.stages.stg1 import StgOne

# to register pipes and generators as SamplesSequence methods,
# you just need to import the modules
import my_project.sequences.gen
import my_project.sequences.pipe
```

Then, you can list the pipelime items, show their help and run them all with:

```bash
$ pipelime -m my_project list
$ pipelime -m my_project help CmdZero
$ pipelime -m my_project cmd-zero --config config.yaml [...]
```

## Custom Entry Points

Though `pipelime -m my_project [...]` works out-of-the-box, you may want to hide it and just run `my_project [...]` instead.

To do so, first add this function to `my_project/__init__.py`:

```python
def plmain():
    from pipelime.cli.main import run_with_extra_modules

    run_with_extra_modules("my_project")
```

Then, set `my_project:plmain` as entry point in your `pyproject.toml`:

```toml
[project.scripts]
my_project = "my_project:plmain"
```

or in your `setup.py` as well:

```python
setup(
    ...
    entry_points={
        "console_scripts": [
            "my_project=my_project:plmain",
        ],
    },
    ...
)
```

Now, after installing your package, you can run:

```bash
$ my_project list
$ my_project help CmdZero
$ my_project cmd-zero --config config.yaml [...]
```
