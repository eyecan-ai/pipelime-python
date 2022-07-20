# Pipelime CLI Cheatsheet

A quick reference to the pipelime CLI.

## Getting Help

| Command | Description |
| ---- | ---- |
| `pipelime` | Base command help |
| `pipelime list` | List commands, operations, stages... |
| `pipelime help <name>` | Show help for a command, an operation, a stage... |

## Basic Usage

`pipelime <command> [OPTIONS]`

| Main Options | Description |
| ---- | ---- |
| `-m` | Search for commands, operations and stages in additional module/packages. |
| `--config` | A YAML/JSON configuration file. |
| `--context` | A YAML/JSON context file. |
| `+<key> <value>`, `++<key> <value>`, `+<key>=<value>`, `++<key>=<value>` | Configuration options. Accepted values are strings, numbers, "true"/"false" (case insensitive), "none"/"null"/"nul" (case insensitive). Value can be omitted for TRUE boolean flags. |
| `@<key> <value>`, `@@<key> <value>`, `@<key>=<value>`, `@@<key>=<value>` | Context options. Accepted values are strings, numbers, "true"/"false" (case insensitive), "none"/"null"/"nul" (case insensitive). Value can be omitted for TRUE boolean flags. |

## Debugging

| CLI Option | Description |
| ---- | ---- |
| `pipelime audit ...` | Inspect the input configuration and context, showing, eg, imports, variables, symbols as well as configuration error, such as missing definitions. |
| `-d -v` | Print the configuration and the context as loaded from files and overridden by the cli. Then show the built command, but **skip the execution**. |
| `-o <file>` | Save the effective processed configuration to YAML/JSON. |

## Multiple Configurations

When the configuration includes directives producing multiple outputs, eg, the `$sweep` directive, pipelime asks if you want to run them all or just the first one.

You can force one of the two choices by using `--run-all` / `--no-run-all`

## Compact Forms

Most complex command options can be written in a compact form,
as specified in every command help. Here a quick reference:

| Option Type | Usual Compact Form | Corresponding Extended Definition | Notes |
| ---- | ---- | ---- | ---- |
| Input Dataset | `+i <folder>[,<skip_empty>]` | `++input.folder <folder> [++input.skip_empty <skip_empty>]` | `<skip_empty>` is an optional flag to skip empty samples. |
| Output Dataset | `+o <folder>[,<exists_ok>[,<force_new_files>]]` | `++output.folder <folder> [++output.exists_ok <exists_ok> [++output.serialization.override.DEEP_COPY null]]` | when `<force_new_files>` is TRUE the output dataset will not contain remote references nor hard/soft links. |
| Multiprocessing | `+g <num_workers>[,<prefetch>]` | `++grabber.num_workers <num_workers> [++grabber.prefetch <prefetch>]` | Both values should be positive integers. |
| Dataset Splits | `+s <fraction>[,<folder>]`, `+s <length>[,<folder>]` | `++splits.fraction <fraction> [++splits.output.folder <folder>]`, `++splits.length <length> [++splits.output.folder <folder>]` | `<fraction>` must be between 0 and 1, while `<length>` is a positive integer. One split may have `null` length to get all remaining samples. |

## Common Tasks

| Description | Command |
| ---- | ---- |
| Deep copy | `pipelime clone +i <input> +o <output>,false,true` |
| Dataset concatenation | `pipelime cat +i <input_1> +i <input_2> +i <input_3> +o <output>` |
| Train/test/val splits | `pipelime split +i <input> +s 0.8,train +s 0.1,test +s null,val` |
| Dataset shuffling, sampling and reduction | `pipelime split +i <input> +s 0.5,<output> +shf +ss 3` |
| Split by query | `pipelime split-query +i <input> +q <dictquery> +os <output>` |
| Split by value | `pipelime split-query +i <input> +k <sample_key> +o <output_base_path>` |
| Generate a basic validation schema | `pipelime validate +i <input>` |
| Upload to a remote storage | `pipelime remote-add +i <input> +o <output> +r s3://user:password@host:port/bucket` |
