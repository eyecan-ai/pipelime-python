Welcome to pipelime's docs!
===========================

<p align="center">
    <img src="_static/pipelime_logo.svg" alt="pipelime" width="30%" height="30%" />
</p>

Hi!

Yeah, I know, you are here because you have no idea how this thing works.

You saw the code and thought "Well, where should I start?".

It's completely normal, even I, when I was trying to use this package, I thought the same.
That's why I spent a lot of minutes (around 3) to write this description,
to let you know that you're not alone.

Pipelime is a full-fledge framework for data science. Read your datasets, manipulate them, write back to disk or upload to a remote data lake. Then build up your dataflow with Piper and manage the configuration with Choixe. Finally, embed your custom commands into the Pipelime workspace, to act both as dataflow nodes and advanced command line interface.

Now that you got the gist, tell me how can I help you.

Remember: _If life gives you lemons, use Pipelime._

![](https://imgs.xkcd.com/comics/data_pipeline.png)

Getting Started
===============

Here I can show you the basic usage!

Common problems
===============

Too many to list them here, find them on Github Issues.

Table of Contents
===============
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
```
