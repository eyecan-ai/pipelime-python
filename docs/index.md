# Welcome to pipelime's docs!

<p align="center">
    <img src="_static/pipelime_logo.svg" alt="pipelime" width="30%" height="30%" />
</p>

Welcome to **Pipelime**, a swiss army knife for data processing!

In short, Pipelime is a full-fledge **framework** for **data science**: read your datasets,
manipulate them, write back to disk or upload to a remote data lake.
Then build up your **dataflow** with Piper and manage the configuration with Choixe.
Finally, **embed** your custom commands into the Pipelime workspace, to act both as dataflow nodes and advanced command line interface.

Maybe too much for you? No worries, Pipelime is **modular** and you can just take out what you need:
- **data processing scripts**: use the powerful `SamplesSequence` and create your own data processing pipelines, with a simple and intuitive API. Parallelization works out-of-the-box and, moreover, you can easily serialize your pipelines to yaml/json. Integrations with popular frameworks, e.g., [pytorch](https://pytorch.org/), are also provided.
- **easy dataflow**: Piper can manage and execute direct acyclic graphs (DAGs), giving back feedback on the progress through sockets or custom callbacks.
- **configuration management**: Choixe is a simple and intuitive mini scripting language designed to ease the creation of configuration files with the help of variables, symbol importing, for loops, switch statements, parameter sweeps and more.
- **command line interface**: Pipelime can remove all the boilerplate code needed to create a beautiful CLI for you scripts and packages. You focus on *what matters* and we provide input parsing, advanced interfaces for complex arguments, automatic help generation, configuration management. Also, any pipelime command can be used as a node in a dataflow for free!

And... remember: *If life gives you lemons, use Pipelime!*

![](https://imgs.xkcd.com/comics/data_pipeline.png)

## Table of Contents

```{toctree}
:maxdepth: 4
:caption: "Get Started:"

get_started/installation.md
get_started/entities.md
get_started/operations.md
get_started/underfolder.md
```

```{toctree}
:maxdepth: 4
:caption: "Core Components: "

sequences/sequences.md
sequences/items.md
```

```{toctree}
:maxdepth: 4
:caption: "Extending Pipelime: "

operations/intro.md
operations/stages.md
operations/pipes.md
operations/commands.md
```

```{toctree}
:maxdepth: 4
:caption: "Piper: "

piper/dags.md
```

```{toctree}
:maxdepth: 4
:caption: "Choixe: "

choixe/intro.md
choixe/syntax.md
choixe/directives.md
choixe/xconfig.md
```

```{toctree}
:maxdepth: 4
:caption: "Command Line Usage: "

cli/cli.md
```

```{toctree}
:maxdepth: 4
:caption: "Advanced Topics: "

advanced/remotes.md
advanced/validation.md
```

```{toctree}
:maxdepth: 4
:caption: "API Reference:"

api/stages.md
api/operations.md
api/commands.md
api/generated/modules.rst
```
