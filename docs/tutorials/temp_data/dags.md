# Temporary Folders In Your DAGs

One of the most useful feature of Pipelime is the processing of directed acyclic graph (DAG).
If you are not familiar with DAGs, you can read more about them in the [Piper](../../cli/piper.md) section. In this recipe, we will use a very simple configuration of two nodes:
1. the first command generates a toy dataset
2. the second command splits the dataset into train and test sets

```yaml
nodes:
    create:
        toy_dataset:
            toy:
                length: 10
            output: tmp_tutorial/toy_folder
    train_test:
        split:
            input: tmp_tutorial/toy_folder
            splits:
                - 0.8,tmp_tutorial/train
                - 0.2,tmp_tutorial/test
```

Running the DAG above as

```bash
$ pipelime run -c dag.yaml
```

you get a new `tmp_tutorial` folder in your working directory containing three underfolder datasets:
- `toy_folder`: the original toy dataset
- `train`: the train set
- `test`: the test set

Pipelime uses hardlinks whenever possible, so you are not wasting disk space.
Though, once the data is split into train and test sets, the `toy_folder` dataset can be
safely deleted. Indeed, such folder might well be created in the user temporary directory and deleted just after the execution of the DAG.

To this end, we make use of the `$tmp()` Choixe directive (read more about Choixe [here](../../choixe/intro.md)):

```yaml
nodes:
    create:
        toy_dataset:
            toy:
                length: 10
            output: $tmp()/toy_folder
    train_test:
        split:
            input: $tmp()/toy_folder
            splits:
                - 0.8,tmp_tutorial/train
                - 0.2,tmp_tutorial/test
```

Now, running the DAG does not create a `toy_folder` under `tmp_tutorial`, but populates the user temporary directory with your toy data, which are then automatically deleted after DAG execution.

```{tip}
The `$tmp()` directive can be used from CLI as well!

For example:

`$ pipelime split +i input_data +s 0.5,train +s '0.3,$tmp()/null' +s 0.2,test`

will put the first half of samples from `input_data` into the `train` folder and the last 20% into the `test` folder, while the remaining 30% will be skipped.

**NB**: you MUST enclose `'0.3,$tmp()/null'` in *single* quotes and, depending on your shell, escape the dollar sign.
```
