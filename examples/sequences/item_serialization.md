# Item Serialization

The serialization of an item is carried out by the class itself, which knows the file
type and the binary format. The output, however, might be a new file as well as a link
or an address pointing to a remote data lake. The standard approach is to try the
following actions and to stop once one of them succeeds:
1. *remote file* (`REMOTE_FILE`): if remote source addresses are available, they are dumped
1. *hard link* (`HARD_LINK`): if file sources are available, one of them is hard linked
1. *deep copy* (`DEEP_COPY`): if file sources are available, one of them is copy
1. *new file* (`CREATE_NEW_FILE`): a new file is created by serializing the item value

Moreover, a *soft link* (`SYM_LINK`) option can be tried instead of *remote file* and
*hard link*, but only if **explicitly requested**.

To alter how the item is serialized, you can explicitly set the
`Item.serialization_mode` property or use the provided context managers
`pipelime.items.item_serialization_mode` and
`pipelime.items.item_disabled_serialization_modes` (NB: can be used as function
decorators as well!).

Furthermore, the `to_underfolder` sequence operator lets you
specify the serialization mode by key through the `key_serialization_mode` parameter,
eg:
```
seq.to_underfolder(
    "./writer_output",
    key_serialization_mode={
        "image": "CREATE_NEW_FILE",
        "label": "SYM_LINK",
    }
)
```

Finally, any pipelime command using the standard `OutputDatasetInterface` provides a way
to override/disable the standard serialization mode by item type and item key, eg:
```
{
    "serialization": {
        "override": {
            "CREATE_NEW_FILE": ["PngImageItem", "BinaryItem"]
        }
        "disable": {
            "NumpyItem": "HARD_LINK"
        }
        "keys": {
            "mask": "REMOTE_FILE",
            "label": "SYM_LINK",
        }
    }
}
```
