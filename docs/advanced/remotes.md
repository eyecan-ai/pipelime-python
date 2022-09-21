# Remote Data Lakes

When dealing with large amounts of data, it is often necessary to store it in a remote location.
This not only reduces the burden of sharing large datasets, but also allows to efficiently version the data, e.g., on a git repository.
Indeed, using a remote data lake you decouple *data storing* from *data structure*:
* binary blobs are securely backed up on a data lake, e.g., a S3 bucket
* the dataset is versioned and shared as a collection of text files

In this section we will see how to use a remote data lake with pipelime.

## Setup

First, you need a remote location, e.g., a S3 bucket or a shared folder.
Then, make sure you can access it from your machine.
Shared folders should be mounted as a network drive, while to access S3 buckets you usually have multiple options:
- environment variables:
  * access key: AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY, MINIO_ACCESS_KEY
  * secret key: AWS_SECRET_ACCESS_KEY, AWS_SECRET_KEY, MINIO_SECRET_KEY
  * session token: AWS_SESSION_TOKEN
- configuration files:
  * ~/.aws/credentials
  * ~/[.]mc/config.json

## Upload Your Data

To upload a dataset to a remote location, pipelime provides the `remote-add` command, which takes a dataset as input, uploads the data and writes a new dataset including only paths to the data lake. The full range of options is as follows:
- `input`: the input dataset
- `output`: the output dataset
- `grabber`: multiprocessing options
- `remotes`: one or more remote locations, e.g., `s3://user:password@host:port/bucket`
- `keys`: the item keys to upload (leave empty to upload all items)
- `start`, `stop`, `step`: input slicing options to limit the sample to upload (but the whole dataset is always written to disk)

Likewise, the `remote-upload` stage is available with a similar interface.

Eventually, you end up with a sequence where the items have multiple sources, i.e., the local file path and one or more remote paths. When writing to disk, though, a single text file is created for each item, including only the remote addresses.
Please note that this behavior can be altered by setting different [serialization modes](../sequences/items.md#serialization-modes).

Instead, on the remote location you will find a single folder containing all the files and some metadata.
The file names come from a hash computed on the file content itself, e.g., SHA 256, so that you can safely upload the same file multiple times without wasting space.
At the same time, you can split the dataset into multiple collections and upload them independently, without worrying about collisions,
as well as upload different versions of an item data without overwriting the previous one.

## Sharing And Versioning

Once uploaded, your dataset is just a collection of text files, so you can version and share it as you would do with any other folder of text files.
Also, no special care is needed when reading it. Pipelime recognizes the remote paths and automatically downloads the data when needed from the first available source. This means that you can even provide multiple remote locations if users have different access rights.

Finally, downloading and saving the binary data to disk is straighforward.
Just use the `clone` command and set the serialization mode to anything but `REMOTE_FILE`, for example:

```bash
$ pipelime clone +i remote_only_dataset +o.folder local_dataset +o.serialization.override.CREATE_NEW_FILE null
```

```{attention}
To remove a remote source from your dataset, you can use the `remote-remove` command, which has an interface similar to `remote-add`.
However, **no action** is performed on the remote location, since we don't know if other datasets are using the same files.
Therefore, you should manually remove the remote files when no longer needed.
```
