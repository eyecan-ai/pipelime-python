# XConfig

```{admonition} Warning
:class: warning
Consider switching to the new `Choixe` class! It's more lightweight and more flexible.
```

All **Choixe** functionalities can be accessed with the `XConfig` class. You can construct an `XConfig` from any python mapping, by simply passing it to the constructor:

```python
from choixe import XConfig

# A dictionary containing stuff
data = {
    "alpha": {
        "a": 10,
        "b": -2,
    },
    "beta": {
        "a": "hello",
        "b": "world",
    },
}

# Instance an XConfig
cfg = XConfig(data)
```

## Interaction

An `XConfig` is also a python `Mapping` and has all the methods you would expect from a plain python `dict`.

In addition, you can get/set its contents with the dot notation (if you know the keys at code time) like:

```python
cfg.alpha.b
# -2

cfg.alpha.new_key = 42
cfg.alpha
# {"a": 10, "b": -2, "new_key": 42}
```

You can also use `deep_get` and `deep_set` to get/set deep content using [pydash](https://pydash.readthedocs.io/en/latest/deeppath.html) keys, useful if you don't know the keys at code time. The `deep_set` method also have a flag that disables the setting of the new value if this involves creating a new key.

```python
cfg.deep_get("alpha.b")
# -2

cfg.deep_set("alpha.new_key", 42)
cfg.deep_get("alpha")
# {"a": 10, "b": -2, "new_key": 42}

# This should be a NoOp, since "another_new_key" was not already present.
cfg.deep_set("alpha.another_new_key", 43, only_valid_keys=True)
cfg.deep_get("alpha")
# {"a": 10, "b": -2, "new_key": 42}
```

There is also a `deep_update` method to merge two configurations into one. The `full_merge` flag enables the setting on new keys.

```python
data = XConfig({
    "a": {"b": 100},
    "b": {"a": 1, "b": [{"a": -1, "b": -2}, "a"]},
})
other = XConfig({"b": {"b": [{"a": 42}], "c": {"a": 18, "b": 20}}})
data.deep_update(other)
# {
#     "a": {"b": 100},
#     "b": {"a": 1, "b": [{"a": 42, "b": -2}, "a"]},
# }

data.deep_update(other, full_merge=True)
# {
#     "a": {"b": 100},
#     "b": {"a": 1, "b": [{"a": 42, "b": -2}, "a"], "c": {"a": 18, "b": 20}}
# }
```

At any moment, you can convert the XConfig back to a dictionary by calling `to_dict`.

```python
plain_dict = cfg.to_dict()
type(plain_dict)
# dict
```

## I/O

`XConfig` provides a simplified interface to load/save from/to a file. The format is automatically inferred from the file extension.

```python
cfg = XConfig.from_file("path/to/my_cfg.yml")
cfg.save_to("another/path/to/my_cfg.json")
```


## Processing
At the moment of creation, the `XConfig` content will be an exact copy of the plain python dictionary used to create it, no check or operation is performed on the directives, they are treated just like any other string.

To "activate" them, you need to **process** the `XConfig`, by calling either `process` or `process_all`, passing an appropriate **context** dictionary.

`process` will compile and run the configuration **without branching** (i.e. sweeps nodes are disabled), and return the processed `XConfig`.

`process_all` will compile and run the configuration **with branching** and return a list of all the processed `XConfig`.

```python
cfg = XConfig.from_file("config.yml")

output = cfg.process()
outputs = cfg.process_all()
```

### Unsafe processing variant

The `process` and `process_all` methods always return a new `XConfig` object, but sometimes you might want to use an `XConfig` to create a completely different object that cannot be represented as a `XConfig`, like a `numpy.ndarray` or a `pydantic.BaseModel` or anything else. In this case, you can use the `unsafe_process` and `unsafe_process_all` methods, which will return the result of the processing, without creating a new `XConfig` object. 

These methods don't perform any validation on the result, all typing and content checks are left to the user, and you should use them only if you want to allow different types of objects to be returned by the choixe processing.

Example:

```python
import numpy as np

cfg = {
    "$call": "numpy.array",
    "$args": {
        "object": [1, 2, 3],
    }
}
xcfg = XConfig(cfg)

# xcfg.process() <--- This will raise an error

result = xcfg.unsafe_process() # <--- This will return a numpy.ndarray

assert isinstance(result, np.ndarray)
assert np.array_equal(result, np.array([1, 2, 3]))
```

## Inspection
What if you just loaded an XConfig and are about to process it, only, you don't know what to pass as **context**, what environment variables are required, or what files are going to be imported. You can use the `inspect` method, to get some info on the `XConfig` that you are about to process.

The result of `inspect` is an `Inspection`, containing the following fields:
- `imports` - A `set` of all the absolute paths imported from the inspected `XConfig`.
- `variables` - An uninitialized structure that can be used as context, containing all variables and loop iterables, and their default value if present (`None` otherwise).
- `environ` - An uninitialized structure containing all environment variables and their default value if present (`None` otherwise).
- `symbols` - The `set` of all dynamically imported python files/modules.
- `processed` - A `bool` value that is true if the `XConfig` contains any **Choixe directive** and thus needs processing.

## Validation

At the moment of creation - whether from file or from a plain dictionary, you can specify an optional `Schema` object used for validation. More details [here](https://github.com/keleshev/schema).

Validation is enabled only when a schema is set.

To validate an `XConfig` you can use `is_valid` to know if the configuration validates its schema. You can also use `validate` to perform full validation (enabling side effects).
