# Overview

Pipelime is equipped with a complete framework to ease the creation of any command line interface.
To get the most out of it, first you should feel comfortable with the basic concepts.
So, just run `pipelime` in your shell and dive into the documentation!

## Basic Usage

The following options applies to main `pipelime` command. You recognize them because they start with `--` or `-`:
- `--help`, `-h`: show the help message and exit.
- `--version`: show pipelime version number and exit.
- `--dry-run`, `-d`: load the configuration, create the command object, but skip the actual execution.
- `--verbose`, `-v`: increase verbosity level, really useful for **debugging**, especially when used in combination with `--dry-run`.
- `--output`, `-o`: output file path (yaml/json) where to save the effective configuration.
- `--run-all`, `--no-run-all`: in case of multiple configurations, e.g., when a `$sweep` is present, run them all; otherwise, run only the first one. If not specified, user will be notified if multiple configurations are found.
- `--module`, `-m`: additional module and packages where user-defined commands, sequence generators, piped operations and stages are defined. This option can be specified multiple times.
- `--config`: path to a yaml/json file with all the parameters required by the command.
- `--context`: path to a yaml/json file with the context needed by Choixe to resolve variables, for loops etc.

As we will see in a moment, the configuration file is in fact merged with command line arguments
starting with `++` or `+`. Likewise, context file is merged with command line arguments starting with `@@` or `@`.
Also, after a double slash `//`, both `++`/`+` and `@@`/`@` can be used to specify context arguments.

Beside this bunch of options, there is also a list of CLI subcommands:
- `help`, `h`: same as `--help`, but can be used also to get help on a pipelime command, a sequence operator or a stage (see [Get Help](#get-help)).
- `list`, `ll`, `l`: list all the available pipelime commands, sequence operators and stages. If `--module` is specified, only the symbols defined in the specified module(s) will be listed.
- `list-commands`, `list-cmds`, `list-cmd`, `lc`: same as `list`, but printing only pipelime commands.
- `list-operators`, `list-ops`, `list-op`, `lo`: same as `list`, but printing only sequence generators and piped operators.
- `list-stages`, `list-stgs`, `list-stg`, `ls`: same as `list`, but printing only stages.
- `audit`, `a`: inspect the given configuration and context, if any, printing the effective configuration and missing definitions. A wizard to write a new valid context is started afterwards.
- `wizard`, `w`: start a wizard to write a configuration file for a given pipelime command.
- `exec`, `exe`, `x`, `e`: execute a configuration where the command is the top-level key, useful when you want to ship a configuration for a single command to run.

Now we are ready to explore some common scenarios.

### Get Help

All the `list*` commands can be used to retrieve the available pipelime interfaces, i.e., commands, sequence operators and stages,
limiting the search to specific modules with `-m`. For example:

```bash
$ pipelime list-stg
# >>>
# ---Sample Stages
# albumentations pipelime.stages.augmentations.StageAlbumentations     Sample augmentation via Albumentations.
# compose        pipelime.stages.base.StageCompose                     Applies a sequence of stages.
# duplicate-key  pipelime.stages.key_transformations.StageDuplicateKey Duplicate an item.
# forget-source  pipelime.stages.item_sources.StageForgetSource        Removes data sources, ie, file paths or remotes, from items.
# identity       pipelime.stages.base.StageIdentity                    Returns the input sample.
# item-info      pipelime.stages.item_info.StageItemInfo               Collects item infos from samples.
#                                                                      WARNING: this stage CANNOT be combined with MULTIPROCESSING.
# format-key     pipelime.stages.key_transformations.StageKeyFormat    Changes key names following a format string.
# filter-keys    pipelime.stages.key_transformations.StageKeysFilter   Filters sample keys.
# lambda         pipelime.stages.base.StageLambda                      Applies a callable to the sample.
# remap-key      pipelime.stages.key_transformations.StageRemap        Remaps keys in sample preserving internal values.
# replace-item   pipelime.stages.item_replacement.StageReplaceItem     Replaces items in sample preserving internal values.
# remote-upload  pipelime.stages.item_sources.StageUploadToRemote      Uploads the sample to one or more remote servers.
```

Where each line shows:
- the title of the stage you can use in your configuration file.
- the full name of the class implementing the stage, if needed.
- a short description of the stage, i.e., the docstring of the class.

To get help on a specific command, operator or stage, just type `help`:

```bash
$ pipelime help filter-keys
# >>>
# --Sample Stage
#                                                 filter-keys
#                              (*, key_list: Sequence[str], negate: bool = False)
#                                             Filters sample keys.
#  Fields     Description                                                             Type            Default
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  key_list   ▶ List of keys to preserve.                                             Sequence[str]   ✗
#  negate     ▶ TRUE to delete `key_list`, FALSE delete all but keys in `key_list`.   bool            False
#                             pipelime.stages.key_transformations.StageKeysFilter
```

```{tip}
You can autogenerate similar help messages for **any** class derived from `pydantic.BaseModel`!

Just print your class with `pipelime.cli.pl_print`.
```

### Create A New Configuration

To create a new configuration file, just run `pipelime wizard [command]` and follow the instructions:

```bash
$ pipelime wizard clone
```

First, an help message is printed:
- enclose values in `"` or `'` quotes to enforce string values.
- use `[` to start a sequence, then `]` to end it.
- likewise, `{` to start a mapping and `}` to end it. Each key-value pair must be separated by a colon `:`.
- `< [model.class.path]` to insert a pydantic model (should be explicitly listed in the type list).
- `? [class.path]` to begin a wizard configuration for a Choixe `$call` directive, e.g., an object to be instantiated.
- `! [class.path]` to add a Choixe `$symbol` directive.
- `# [name]` to begin a wizard configuration for a pipelime command, stage or operation.
- `c# [name]`, `s# [name]`, `o# [name]` as above, but specifying the type.

```{warning}
Since you may use Choixe directives and other fancy stuffs,
**no validation nor parsing is performed on the data you provide!**

See next section to see how to do it.
```

```{tip}
Anytime you have to insert a class path, you can either use the usual python dot notation,
or provide a **path to a python file**, e.g., `path/to/mymodule.py:MyClass`. Though, the latter should be use with **caution**, since multiprocessing execution is not supported.
```

```{note}
The wizard is intended to be used with pipelime commands, however, you may find it works
also with stages and operations.

Moreover, you can run it on any class derived from `pydantic.BaseModel`!
Just give the class type to `pipelime.cli.wizard.model_cfg_wizard`.
```

### Validate A Configuration And Write A Context

Once you get your new configuration file, it's time to validate it and write a context, if needed.
Run `pipelime audit` on your configuration:

```bash
$ pipelime audit --config config.yaml
```

If the configuration is valid, you will see the list of internal imports, variables and symbols.
Then, if anything is missing, you can immediately start a wizard to write a context file.

```{tip}
`pipelime audit` works with any yaml/json file using Choixe.
```

### Merge Options From File And Command Line

If you run `pipelime help` on a command, you often see the options in a tree-like structure:

```bash
$ pipelime help clone
# >>>
# ---Pipelime Command
#                                                     clone
#                         (*, i: pipelime.commands.interfaces.InputDatasetInterface, o:
# pipelime.commands.interfaces.OutputDatasetInterface, g: pipelime.commands.interfaces.GrabberInterface = None)
#                 Clone a dataset. You can use this command to create a local copy of a dataset
#               hosted on a remote data lake by disabling the `REMOTE_FILE` serialization option.
#
#   Fields                  Description            Type                    Piper Port     Default
#  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#   input / i               ▶ The input dataset.                           📥 INPUT       ✗
#                           ━━━━━ Compact form:
#                           `<folder>[,<skip_emp
#                           ty>]`
#     folder                ▶ Dataset root         Path                    📐 PARAMETER   ✗
#                           folder.
#     merge_root_items      ▶ Adds root items as   bool                    📐 PARAMETER   True
#                           shared items to each
#                           sample (sample
#                           values take
#                           precedence).
# ...
#   output / o              ▶ The output                                   📦 OUTPUT      ✗
#                           dataset.
#                           ━━━━━ Compact form:
#                           `<folder>[,<exists_o
#                           k>[,<force_new_files
#                           >]]`
#     folder                ▶ Dataset root         Path                    📐 PARAMETER   ✗
#                           folder.
# ...
#     serialization         ▶ Serialization                                📐 PARAMETER   override={}
#                           modes for items and                                           disable={} keys={}
#                           keys.
#       override            ▶ Serialization        Mapping[str,            📐 PARAMETER   {}
#                           modes overridden for   Union[str,
#                           specific item types,   Sequence[str],
#                           eg,                    NoneType]]
#                           `{CREATE_NEW_FILE:
#                           [ImageItem,
#                           my.package.MyItem,
#                           my/module.py:OtherIt
#                           em]}`. A Null value
#                           applies to all
#                           items.
```

The same structure is what you should follow both when writing a configuration file and
when providing options from the command line. To do so, you can adopt a
[pydash-like notation](https://pydash.readthedocs.io/en/latest/deeppath.html):
- `.<key>` to access a mapped field.
- `[<idx>]` to index a list entry.

As for the values:
- `true` and `false` are converted to booleans (case insensitive).
- `none`, `null` and `nul` are interpreted as `None` (case insensitive).
- numbers are converted to integers or floats, depending on the presence of a decimal point.

Also, options declared with no value are interpreted as `True` boolean flags.

### Executing A Command

Once you have a valid configuration file, you can run the command as `pipelime <command>`
followed by the configuration and context:

```bash
$ pipelime clone --config config.yaml --context context.yaml +i input @the_answer 42
```

In the example above we are running `clone` using the parameters in `config.yaml` and the context
in `context.yaml`. We are also overriding the input dataset with the folder `input` and
the Choixe variable `the_answer` with the value `42`.

Though it works, there is a serious **drawback**: if you distribute `config.yaml` to other users,
you also have to say them to run the `clone` command, since it might not be easy to infer it
from the configuration file. Instead, add `clone` as a top-level key in `config.yaml`:

```yaml
clone:
  input:
    folder: any_input
  output:
    folder: output_$var(the_answer)
```

And now run again with `pipelime exec --config config.yaml --context context.yaml +i input @the_answer 42`.
