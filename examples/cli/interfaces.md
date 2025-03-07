# Standard Parameter Interfaces

Pipelime commands are built for best modularity and reusability, so common parameters,
such as input and output datasets, are defined in standard interfaces. Here a brief
overview.

## Input Dataset And Schema Validation

An input dataset is defined by:
* `folder`: the root folder of the dataset
* `merge_root_items`: wether shared root items should be added to each sample
* `schema`: the optional schema validation

The validation schema is mainly defined by the `schema.sample_schema` key, which is
basically a mapping from sample keys to item type, eg:
```
{
    "schema": {
        "sample_schema": {
            "image": {
                "class_path": "ImageItem",
                "is_optional": False,
            },
            "label": {
                "class_path": "TxtNumpyItem",
                "is_optional": True,
            },
        }
    }
}
```

Fine-grained validation can be performed by adding validator callables or, instead of
the dictionary above, a full-fledged pydantic model, eg:
```
$ my_schema.py
----------------------------------------------------
from pydantic import BaseModel, validator
from pipelime.items import ImageItem, TxtNumpyItem
from typing import Optional

class MySchema(BaseModel):
    image: ImageItem
    label: Optional[TxtNumpyItem] = None

    @validator("image")
    def check_image_size(cls, v):
        if v.shape[0] != 224 or v.shape[1] != 224:
            raise ValueError("Image must be 224x224")
        return v
```
Then:
```
{
    "schema": {
        "sample_schema": "my_schema.py:MySchema"
    }
}
```

**TIP**: to get a minimal schema for an existing dataset, try the following command:

```
$ pipelime validate +input.folder ../../tests/sample_data/datasets/underfolder_minimnist +max_samples 1
```

## Output Dataset And Serialization Modes

Like input datasets, an output dataset is mainly defined by the `folder` path and an
optional `schema` definition. Moreover, you can fine-tune how items are actually
serialized to disk.

The standard serialization procedure tries the following sequence of actions and stops
when one of them succeeds:
1. *hard link* (`HARD_LINK`): if file sources are available, one of them is hard linked
1. *deep copy* (`DEEP_COPY`): if file sources are available, one of them is copy
1. *new file* (`CREATE_NEW_FILE`): a new file is created by serializing the item value

Moreover, a *soft link* (`SYM_LINK`) option can be tried instead of 
*hard link*, but only if **explicitly requested**.

To alter this behavior, you can set the `serialization` option so as to override,
disable or force the desired mode, eg:
```
{
    "serialization": {
        "override": {
            "CREATE_NEW_FILE": [ "PngImageItem", "BinaryItem" ]
        }
        "disable": {
            "NumpyItem": "HARD_LINK"
        }
        "keys": {
            "mask": "DEEP_COPY",
            "label": "SYM_LINK",
        }
    }
}
```

## The Grabber

The grabber interface provides a easy way to distribute data iteration on multiple
processes. The whole pipeline, included the item serialization, is executed in parallel,
but be aware that the process spawning overhead can be significant, so parameters should
be carefully tuned. The definition is as follows:
* `num_workers`: the number of processes to spawn. If negative, the number of (logical)
cpu cores is used.
* `prefetch`: the number of samples loaded in advanced by each worker, which might be
useful if the parent command has to post-process the data.
