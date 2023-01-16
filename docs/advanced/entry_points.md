# Project Entry Points

Using pipelime, you can skip all the boilerplate code needed to create a command line interface for your own python package.
You just have to wrap your logic within `PipelimeCommand`s, `SampleStage`s, pipes and generators,
then pipelime will take care of the rest. The advantages are clear:
* less code: pipelime takes care of managing input configurations, contexts, help messages, etc.
* less bugs: pipelime is tested and maintained, so you can be sure that it works as expected
* less time: you can focus on your logic, not on the boilerplate code
* more flexibility: you can mix parameters from many sources and use `-d` (dry run) to test your code without running it, `-vv` to debug how options are parsed and merged, `-o` to save the actual configuration etc.
* easier testing: to test a pipelime command you just need to instantiate an object and _call_ it, no need to mock the command line interface
* piper ready: any pipelime command can be used within a piper DAG to create complex ETL pipelines

## Making A CLI From A Single Module

If you put all your code in a single module, Pipelime can create a CLI for you automatically.
A minimal example looks like this:

```python
from typing import Optional
from pydantic import Field

import pipelime.cli
from pipelime.piper import PipelimeCommand


class HelloWorldCommand(PipelimeCommand, title="hello"):
    """Greets the world and the user, if any."""

    user_name: Optional[str] = Field(None, alias="u", description="The name of the user to greet.")

    def run(self):
        print("Hello World!")
        if self.user_name:
            print(f"Hello {self.user_name}!")


if __name__ == "__main__":
    pipelime.cli.run()  # ⬅️ THE MAGIC HAPPENS HERE
```

Copy to a file named `hello.py` and run it with:

```bash
$ python hello.py
```

You should see the standard Pipelime help message, but customized with the name of the module. Moreover, you can add a custom description of your CLI by simply wrapping the `pipelime.cli.run()` call in a function with a docstring:

```python
...

def main():
    """My Awesome Hello World CLI."""
    pipelime.cli.run()

if __name__ == "__main__":
    main()
```

Indeed, this is a full-fledged Pipelime CLI just renamed for you:

```bash
$ python hello.py list
>>
━━━━━ Pipelime Commands
hello hello.py:HelloWorldCommand Greets the world and the user, if any.

$ python hello.py hello
>>
Hello World!

Command executed in 0ms 116us 200ns

`hello` outputs:

$ python hello.py hello +u "John"
>>
Hello World!
Hello John!

Command executed in 0ms 172us 0ns

`hello` outputs:
```

Also, any Pipelime object (stages, pipes, items, etc.) defined in the module is automatically registered, so you can use them in your pipelines and DAGs out-of-the-box!

## Making A CLI For A Full-Fledged Package

Consider this package structure:

```
my_project
├─ __init__.py
├─ commands.py
├─ stages.py
└─ ...
```

Where `commands.py` contains the commands we want to expose through the CLI
and `stages.py` some new stages.
Though just calling `pipelime.cli.run()` might work, maybe now you want to add extra information, such as a version number.
A first simple solution is to declare a `PipelimeApp` in `commands.py`:

```python
from pipelime.cli import PipelimeApp
from pipelime.piper import PipelimeCommand

app = PipelimeApp(app_version="0.1.0")

class MyCommand0(PipelimeCommand, title="cmd0"):
    ...

class MyCommand1(PipelimeCommand, title="cmd1"):
    ...

if __name__ == "__main__":
    app()
```

The `app` object can be used as entry point for your package, just add to `pyproject.toml`:

```toml
[project.scripts]
myp = "my_project.commands:app"
```

So that you can run, eg:

```bash
$ myp --version
$ myp cmd0 help
```

However, if look at the help message, your app is called "My_package" and there is no general overview on what this project does. Let's fix that:

```python
from pipelime.cli import PipelimeApp
from pipelime.piper import PipelimeCommand

app = PipelimeApp(
    app_name="MyProject",
    app_description="This is my awesome project which does NOTHING USEFUL.",
    app_version="0.1.0"
)

class MyCommand0(PipelimeCommand, title="cmd0"):
    ...

class MyCommand1(PipelimeCommand, title="cmd1"):
    ...
```

Now remember that `app` is just the Pipelime CLI renamed. Therefore, all the standard pipelime commands and stages are available, eg:

```bash
$ myp help map
$ myp help pipe
```

Unfortunately, if you want to run a `map` command with a custom stage from `my_project/stages.py`
you need to import the module first. The `import` statement can be placed in `__init__.py`
or you can give a list of modules to import to the `PipelimeApp` initializer:

```python
from pipelime.cli import PipelimeApp
from pipelime.piper import PipelimeCommand

app = PipelimeApp(
    "my_project.commands",
    "my_project.stages",
    app_name="MyProject",
    app_description="This is my awesome project which does NOTHING USEFUL.",
    app_version="0.1.0"
)

class MyCommand0(PipelimeCommand, title="cmd0"):
    ...

class MyCommand1(PipelimeCommand, title="cmd1"):
    ...
```

## Best Practices

When things get more complex, defining a `PipelimeApp` in `commands.py` might not be the best solution. Indeed, commands and stages might be split in multiple modules, as in this example:

```
my_project
├─ __init__.py
├─ commands
|  ├─ __init__.py
|  ├─ cmd_zero.py
|  └─ cmd_one.py
├─ stages
|  ├─ __init__.py
|  ├─ stg_zero.py
|  └─ stg_one.py
├─ ...
```

so you might want to put the main entry point in a separate module or even in the base `my_project/__init__.py`. Anyhow, a good practice is to encapsulate the call in a function which will be the actual entry point:

```python
def app():
    """A general description of the app."""

    from pipelime.cli import PipelimeApp

    app = PipelimeApp("my_project.commands", "my_project.stages", app_version="0.1.0")
    app()
```

Note that, instead of importing a list of modules, we imported the subpackages "my_project.commands" and "my_project.stages". This way, you can import all the _public_ commands and stages in the `__init__.py` of each subpackage, and the global `app` will automatically discover them.
