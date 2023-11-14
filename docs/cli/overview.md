# Overview

Pipelime is equipped with a complete framework to ease the creation of any command line interface.
To get the most out of it, first you should feel comfortable with the basic concepts.
So, just run `pipelime` in your shell and dive into the documentation!

## Basic Usage

The following options applies to main `pipelime` command. You recognize them because they start with `--` or `-`. They can be grouped in the following categories:
- **general options**:
  - `--help`, `-h`: show the help message and exit.
  - `--version`: show pipelime version number and exit.
  - `--run-all`, `--no-run-all`: in case of multiple configurations, e.g., when a `$sweep` is present, run them all; otherwise, run only the first one. If not specified, user will be notified if multiple configurations are found.
  - `--checkpoint`, `--ckpt`, `-k`: path to the optional checkpoint folder where to save the execution state. If not specified, no checkpoint is saved. Look [here](./piper.md#resuming-a-dag-from-a-checkpoint) for more details.
- **debugging and automation**:
  - `--dry-run`, `-d`: load the configuration, create the command object, but skip the actual execution.
  - `--verbose`, `-v`: increase verbosity level, really useful for **debugging**, especially when used in combination with `--dry-run`. Can be specified multiple times.
  - `--keep-tmp`, `-t`: keep temporary folders created by Pipelime. See [this example](../tutorials/temp_data/tmp_command.md) for more details.
  - `--output`, `-o`: output file path (yaml/json) where to save the effective configuration.
  - `--output-ctx`: output file path (yaml/json) where to save the effective context.
- **configuration**:
  - `--module`, `-m`: additional module and packages where user-defined commands, sequence generators, piped operations and stages are defined. This option can be specified multiple times.
  - `--config`, `-c`: path to a yaml/json file with all the parameters required by the command.
  - `--context`: path to a yaml/json file with the context needed by Choixe to resolve variables, for loops etc. It can be automatically loaded if named `context*.[yaml|yml|json]` and placed in the same folder of the configuration file.

As we will see in a moment, the configuration file is in fact merged with command line arguments
starting with `++` or `+`. Likewise, context file is merged with command line arguments starting with `@@` or `@`.
Also, after a double slash `//`, both `++`/`+` and `@@`/`@` can be used to specify context arguments.

Beside this bunch of options, there is also a list of CLI subcommands:

| Subcommand | Description | Aliases |
| --- | --- | --- |
| `help` | Show help for a pipelime command, a sequence operator or a stage (see [Get Help](#get-help)). | `h` |
| `list` | List all the available pipelime commands, sequence operators and stages. If `--module` is specified, only the symbols defined in the specified module(s) will be listed. | `ll`, `l`, `ls` |
| `list-commands` | Same as `list`, but prints only pipelime commands. | `list-cmds`, `list-cmd`, `lc`, `ls-cmds`, `ls-cmd`, `lsc` |
| `list-operators` | Same as `list`, but prints only sequence generators and piped operators. | `list-ops`, `list-op`, `lo`, `ls-ops`, `ls-op`, `lso` |
| `list-stages` | Same as `list`, but prints only stages. | `list-stgs`, `list-stg`, `lst`, `ls-stgs`, `ls-stg`, `lss` |
| `audit` | Inspect the given configuration and context, if any, printing the effective configuration and missing definitions. | `a` |
| `exec` | Execute a configuration where the command is the top-level key, useful when you want to ship a configuration for a single command to run. | `exe`, `x`, `e` |

Now we are ready to explore some common scenarios.

### Get Help

All the `list*` commands can be used to retrieve the available pipelime entities, i.e., commands, sequence operators and stages,
limiting the search to specific modules with `-m`. For example:

```bash
$ pipelime list-stg
```

```bash
>>>
━━━━━ Sample Stages
albumentations pipelime.stages.augmentations.StageAlbumentations     Sample augmentation via Albumentations.
compose        pipelime.stages.base.StageCompose                     Applies a sequence of stages.
duplicate-key  pipelime.stages.key_transformations.StageDuplicateKey Duplicate an item.
forget-source  pipelime.stages.item_sources.StageForgetSource        Removes data sources, ie, file paths or remotes, from items.
identity       pipelime.stages.base.StageIdentity                    Returns the input sample.
item-info      pipelime.stages.item_info.StageItemInfo               Collects item infos from samples.
                                                                     WARNING: this stage CANNOT be combined with MULTIPROCESSING.
format-key     pipelime.stages.key_transformations.StageKeyFormat    Changes key names following a format string.
filter-keys    pipelime.stages.key_transformations.StageKeysFilter   Filters sample keys.
lambda         pipelime.stages.base.StageLambda                      Applies a callable to the sample.
remap-key      pipelime.stages.key_transformations.StageRemap        Remaps keys in sample preserving internal values.
replace-item   pipelime.stages.item_replacement.StageReplaceItem     Replaces items in sample preserving internal values.
remote-upload  pipelime.stages.item_sources.StageUploadToRemote      Uploads the sample to one or more remote servers.
```

Where each line shows:
- the title of the stage you can use in your configuration file.
- the full name of the class implementing the stage, if needed.
- a short description of the stage, i.e., the docstring of the class.

To get help on a specific command, operator or stage, just type `help`:

```bash
$ pipelime help pipe
```

![](../../images/pipe_help.png)

1. The title reports the name of the command and the full signature
1. The table body describes each argument of the command:
    * Fields: the name and its alias, if any
    * Type: the expected type of the argument
    * Piper Port: wether the argument is an input, an output or a parameter
    * Default: wether the argument has a default value or must be provided by the user
1. The footer shows the full class path of the command class

```{tip}
You can autogenerate similar help messages for **any** class derived from `pydantic.BaseModel`!

Just try `$ pipelime help class.path.to.Model` or `$ pipelime help path/to/module.py:Model`.
```

### Interactive Mode

If you run a command without specifying all the required arguments, an interactive
text user interface is started to help you fill the missing values. Any value is accepted,
you can even input complex data structures, e.g., lists and mappings, as JSON, YAML or
python literals. Try for yourself:

```bash
$ pipelime clone
```

### Validate A Configuration And Write A Context

Once you get your new configuration file, it's time to validate it and write a context, if needed.
Run `pipelime audit` on your configuration:

```bash
$ pipelime audit -c config.yaml
```

If the configuration is valid, you will see the list of internal imports, variables and symbols.

```{tip}
`pipelime audit` works with any yaml/json file using Choixe, so you can use it to validate and parse any configuration file!
To save the final processed configuration, use the `--output/-o` option.
```

### Merge Options From File And Command Line

If you run `pipelime help` on a command, you see that some options are not just raw values.
For instance, you can see the help for `InputDatasetInterface` with `pipelime help pipelime.commands.interfaces.InputDatasetInterface -v` and find out that it accepts more than just a folder path:
* `folder`: dataset root folder
* `merge_root_items`: whether to add root items as shared items to each sample
* `skip_empty`: whether to skip empty samples
* `schema`: sample schema validation

To provide these options in a configuration file you should use nested mappings, e.g.:

```yaml
clone:
  input:
    folder: path/to/dataset
    skip_empty: true
  output:
    folder: path/to/output
    zfill: 6
    exists_ok: true
```

Whereas on the command line you can adopt a
[pydash-like notation](https://pydash.readthedocs.io/en/latest/deeppath.html):
- `.<key>` to access a mapped field.
- `[<idx>]` to index a list entry.

Also, list of values are automatically assigned to the last option.

For example:

```bash
$ pipelime clone +input.folder path/to/dataset +input.skip_empty +output.folder path/to/output +output.zfill 6 +output.exists_ok

$ pipelime cat +i data_0 data_1 data_2 +o output_folder
```

Note how we are using the `+` operator to specify command arguments.
As for the values:
- empty options are interpreted as `True` boolean flags
- `true` and `false` (case insensitive) are converted to booleans
- `none`, `null` and `nul` (case insensitive) are interpreted as `None`
- numbers are converted to integers or floats, depending on the presence of a decimal point

### Executing A Command

Once you have a valid configuration file, you can run the command as `pipelime <command>`
followed by the configuration and context (NB: context file is usually [auto-loaded](#basic-usage)):

```bash
$ pipelime clone -c config.yaml +i input @the_answer 42
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

And now run again with `pipelime exec -c config.yaml --context context.yaml +i input @the_answer 42`.
