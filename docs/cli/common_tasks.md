# Common Tasks

A quick reference to common tasks that can be performed with the pipelime CLI.

## Get Help

How to get help and debug your configuration.

| Command | Description |
| ---- | ---- |
| `pipelime` | Base command help |
| `pipelime lc/lo/lst/` | List built-in commands/operators/stages |
| `pipelime -m module.path lc/lo/lst/` | List built-in commands/operators/stages defined in a python package or module (`.py` file as well!) |
| `pipelime [-m module.path] help <name>` | Show help for a command, an operation etc, possibly from an external module |
| `pipelime audit -c cfg.yaml ...` | Inspect the input configuration and context, showing, eg, imports, variables, symbols as well as configuration error, such as missing definitions. |
| `pipelime -dv ...` | `-v` prints the configuration and the context as loaded from files and overridden by the cli, while `-d` **skip the execution** (dry-run). |

## Short Options

Most complex command options can be written in a compact form,
as specified in every command help. Here a general reference:

| Option Type | Usual Compact Form | Corresponding Extended Definition | Notes |
| ---- | ---- | ---- | ---- |
| Input Dataset | `+i <folder>[,<skip_empty>]` | `++input.folder <folder> [++input.skip_empty <skip_empty>]` | `<skip_empty>` is an optional flag to skip missing samples. |
| Output Dataset | `+o <folder>[,<exists_ok>[,<force_new_files>]]` | `++output.folder <folder> [++output.exists_ok <exists_ok> [++output.serialization.override.DEEP_COPY null]]` | when `<force_new_files>` is TRUE the output dataset will not contain remote references nor hard/soft links. |
| Multiprocessing | `+g <num_workers>[,<prefetch>]` | `++grabber.num_workers <num_workers> [++grabber.prefetch <prefetch>]` | Both values should be positive integers. |
| Dataset Splits | `+s <fraction>[,<folder>]`<br>`+s <length>[,<folder>]` | `++splits.fraction <fraction> [++splits.output.folder <folder>]`<br>`++splits.length <length> [++splits.output.folder <folder>]` | `<fraction>` must be between 0 and 1, while `<length>` is a positive integer. One split may have `null` length to get all remaining samples. |

## Data Processing

Common operations on datasets. To get the most out of any commands, please show the help with `pipelime help <command>`.

### Copy, Zip, Concatenate, Sort

| Description | Command |
| ---- | ---- |
| Deep copy (no links, no remotes) | `pipelime clone +i <input> +o <output>,false,true` |
| Reset indexes (remove missing samples) | `pipelime clone +i <input>,true +o <output>` |
| Dataset concatenation | `pipelime cat +o <output> +i <input_1> +i <input_2> +i <input_3> ...` |
| Sample zipping (items are merged) | `pipelime zip +o <output> +i <input_1> +i <input_2> +i <input_3> ...` |

### Sorting and Filtering

| Description | Command |
| ---- | ---- |
| Sort by classification score [^cscore] | `pipelime sort +i <input> +o <output> +k metadata.classification.score` |
| Sort according to a callable `(Sample) -> Any` [^fnsort][^call] | `pipelime sort +i <input> +o <output> +f class.path.to.callable` |
| Filter by [dictquery match](https://github.com/cyberlis/dictquery) | ``pipelime filter +i <input> +o <output> +q "`metadata.classification.score` > 0.5"`` |
| Filter according to a callable `(Sample) -> bool` [^call] | `pipelime filter +i <input> +o <output> +f class.path.to.callable` |

[^cscore]: Here we assume to have a metadata item such as

    ```yaml
    classification:
        score: 0.9
    ...
    ```

[^fnsort]: The callable must accept a sample and return a value to be used for sorting. You may use `functools.cmp_to_key` to convert a comparison function to a key function.

### Data Manipulation

| Description | Command |
| ---- | ---- |
| Change the names of the item keys | `pipelime map +i <input> +o <output> +s.remap-key.remap.<old_key_1> <new_key_1> +s.remap-key.remap.<old_key_2> <new_key_2> ... +s.remap-key.remove_missing false` |
| Format the names of the item keys | `pipelime map +i <input> +o <output> +s.format-key.key_format prefix_*_suffix` |
| Change image type to jpeg and metadata to toml | `pipelime map +i <input> +o <output> +s.replace-item.key_item_map.image JpegImageItem +s.replace-item.key_item_map.metadata TomlMetadataItem` |
| Leave only the image item in the dataset | `pipelime map +i <input> +o <output> +s.filter-keys.key_list image` |
| Remove a list of keys from every sample | `pipelime map +i <input> +o <output> +s.filter-keys.key_list <key_1> +s.filter-keys.key_list <key_2> ... +s.filter-keys.negate` |
| Apply a user-callable to each sample [^call] | `pipelime map +i <input> +o <output> +s.lambda.func $symbol(\"user.py:my_callable\")` |
| Apply random augmentation on <N> repetitions of the dataset [^alb] | `pipelime pipe +i <input> +o <output> +op.repeat.count <N> +op.map.stage.albumentations.transform transformation.yaml +op.map.stage.albumentations.keys_to_targets.image image` |
| Shuffle, but keep a reference to the original index [^pipe] | `pipelime pipe +i <input> +o <output> +op[0] enumerate +op[1] shuffle` |

However, when operations become too complex, it's easier to write a configuration file.
The following examples can be run with `pipelime exec -c <config.yaml>`

_Change the names of the item keys_

```yaml
map:
    input: <input>
    output: <output>
    stage:
        remap-key:
            remap:
                <old_key_1>: <new_key_1>
                <old_key_2>: <new_key_2>
                ...
            remove_missing: false
```

_Change image type to jpeg and metadata to toml_

```yaml
map:
    input: <input>
    output: <output>
    stage:
        replace-item:
            key_item_map:
                image: JpegImageItem
                metadata: TomlMetadataItem
```

_Remove a list of keys from every sample_

```yaml
map:
    input: <input>
    output: <output>
    stage:
        filter-keys:
            key_list:
                - <key_1>
                - <key_2>
                ...
            negate: true
```

_Apply a user-callable to each sample [^call]_

```yaml
map:
    input: <input>
    output: <output>
    stage:
        lambda:
            func: $symbol("user.py:my_callable")
```

_Apply random augmentation on <N> repetitions of the dataset [^alb]_

```yaml
pipe:
    input: <input>
    output: <output>
    operations:
        repeat:
            count: <N>
        map:
            stage:
                albumentations:
                    transform: transformation.yaml
                    keys_to_targets:
                        image: image
```

_Shuffle, but keep a reference to the original index [^pipe]_

```yaml
pipe:
    input: <input>
    output: <output>
    operations:
      - enumerate
      - shuffle
```

[^call]: If you want to run on multiple processes, the callable must be picklable and referenced through a python class path, eg `my_module.my_callable`.
[^alb]: The `transformation.yaml` file must be a valid [albumentation pipeline](https://albumentations.ai/docs/examples/serialization/#serializing-an-augmentation-pipeline-to-a-json-or-yaml-file).
[^pipe]: The original index is stored in the `~idx` key as `TxtNumpyItem`.

### Split

| Description | Command |
| ---- | ---- |
| Train/test/val splits | `pipelime split +i <input> +s 0.8,train +s 0.1,test +s null,val` |
| Dataset shuffling, subsampling (1 every 3) and reduction (half of the length) | `pipelime split +i <input> +s 0.5,<output> +shf +ss 3` |
| Split by (a boolean) query | `pipelime split-query +i <input> +q <dictquery> +os <output_true> +od <output_false>` |
| Split by value (a new dataset for each value of a given item) | `pipelime split-value +i <input> +k <sample_key> +o <output_base_path>` |

### Piper

Here we assume the context file is named `context*.[yaml|yml|json]` and placed in the same folder of the configuration file.

| Description | Command |
| ---- | ---- |
| Run a dag from config and context files | `pipelime run -c <config.yaml>` |
| Run a only a subset of nodes (must be included and not excluded) | `pipelime run -c <config.yaml> +i node_1 +i node_2 ... +e node_2 +e node_3 ...` |
| Show a dag (needs `Graphviz`, see [installation instructions](../get_started/installation.md)) | `pipelime draw -c <config.yaml>` |
| Show a dag using the [Mermaid](https://mermaid-js.github.io/mermaid/) backend | `pipelime draw -c <config.yaml> +b mermaid` |
| Show a dag with (`+c`) full command names and (`+m`) limited data names' width (any backend) | `pipelime draw -c <config.yaml> +c +m 30` |
| Show a dag anonymizing paths | `pipelime draw -c <config.yaml> +m "/" +ep start` |
| Show a dag at high resolution (`Graphviz` only, see [installation instructions](../get_started/installation.md)) | `pipelime draw -c <config.yaml> +x.G dpi=300` |
| Save a dag to png (any backend) | `pipelime draw -c <config.yaml> +o dag.png` |
| Save a dag to svg or pdf (`Graphviz` only, see [installation instructions](../get_started/installation.md)) | `pipelime draw -c <config.yaml> +o [dag.svg, dag.pdf]` |
| Save a dag to markdown ([Mermaid](https://mermaid-js.github.io/mermaid/) only) | `pipelime draw -c <config.yaml> +o dag.md +b mermaid` |

## Utilities

| Description | Command |
| ---- | ---- |
| Generate a toy dataset of <N> elements | `pipelime toy_dataset +o <output> +t.length <N>` |
| Generate a toy dataset of <N> elements with 64x64 images | `pipelime toy_dataset +o <output> +t.length <N> +t.image_size 64` |
| Generate a toy dataset of <N> elements, then shuffle it | `pipelime pipe +o <output> +op[0]toy_dataset.length 10 +op[1] shuffle`  |
| Measure the time to get an image out of a complex data pipeline [^timeit] | `pipelime timeit +i <input> +o <output> +op pipeline.yaml` |
| Run a general shell command, eg, `paste` two files together | `pipelime shell +c "paste {f0} {f1} > {fout}" +i.f0 <file_0> +i.f1 <file_1> +o.fout <output_file>` |

[^timeit]: Note that:
    1. the output dataset is optional
    2. the `pipeline.yaml` file contains the usual `mapping` or `list of mappings` defining the pipeline in the `pipe` command, eg:
        ```yaml
        - enumerate
        - shuffle
        ...
        ```

## Schema Validation

First, generate a new schema from a dataset:

```bash
$ pipelime validate +i <input>
```

Then, copy-paste the output yaml in your configuration file.

## Remote Data Lakes

| Description | Command |
| ---- | ---- |
| Upload to a S3 remote bucket (user and password) | `pipelime remote-add +i <input> +o <output> +r s3://user:password@host:port/bucket` |
| Upload to a S3 remote bucket (using aws config files) | `pipelime remote-add +i <input> +o <output> +r s3://host:port/bucket` |
| Upload to a (shared/mounted) folder remote (linux) | `pipelime remote-add +i <input> +o <output> +r file://localhost/path/to/folder` |
| Upload to a (shared/mounted) folder remote (windows) | `pipelime remote-add +i <input> +o <output> +r file://localhost/x:/path/to/folder` |
| Upload only a set of item keys | `pipelime remote-add +i <input> +o <output> +r s3://host:port/bucket +k key_1 +k key_2 ...` |
| Upload only the last <N> samples | `pipelime remote-add +i <input> +o <output> +r s3://host:port/bucket +start <N>` |
| Upload every <step> samples from <start> to <stop> | `pipelime remote-add +i <input> +o <output> +r s3://host:port/bucket +start <start> +stop <stop> +step <step>` |
| Remove a remote reference from a dataset. If no other source is available, items are downloaded. The remote data lake is not touched. | `pipelime remote-remove +i <input> +o <output> +r s3://host:port/bucket` |
| Remove a remote reference only from a set of item keys. | `pipelime remote-remove +i <input> +o <output> +r s3://host:port/bucket +k key_1 +k key_2 ...` |
