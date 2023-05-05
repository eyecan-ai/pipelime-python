# Choixe

Beside the `XConfig` class, **Choixe** functionalities can also be accessed with the new `Choixe` class. You can construct a `Choixe` by passing anything you want to its constructor. 

```python
from pipelime.choixe import Choixe

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

# A Choixe instance
chx = Choixe(data)

# Parse into an AST
ast = chx.parse()

# Decode into a plain python object
obj = chx.decode()

# Walk the AST
nodes = chx.walk()

# Process into something else
result = chx.process()

# Process with branching
results = chx.process_all()

# Inspect, looking for variables and imports
inspection = chx.inspect()

# Get what's inside
data_2 = chx.data
assert data is data_2

# File I/O
chx = Choixe.from_file("path/to/file.json")
chx.save_to("path/to/file.yml")
```

## But why?

Don't we have already `XConfig`? Yes, but `Choixe` is a bit different, it does **less**, but nobody really needed all that `XConfig`, but it's also more lightweight and more maintainable. 

Here is a comparison table:

| **Feature**           | **XConfig**                                 | **Choixe**                           |
| --------------------- | ------------------------------------------- | ------------------------------------ |
| Is a python `Mapping` | <span style="color:green">Yes</span>        | <span style="color:red">No</span>    |
| Dot-notation access   | <span style="color:green">Yes</span>        | <span style="color:red">No</span>    |
| Deep access           | <span style="color:green">Yes</span>        | <span style="color:red">No</span>    |
| Schema validation     | <span style="color:green">Yes</span>        | <span style="color:red">No</span>    |
| File I/O              | <span style="color:green">Yes</span>        | <span style="color:green">Yes</span> |
| Parsing               | <span style="color:green">Yes</span>        | <span style="color:green">Yes</span> |
| Decoding              | <span style="color:green">Yes</span>        | <span style="color:green">Yes</span> |
| Walking               | <span style="color:green">Yes</span>        | <span style="color:green">Yes</span> |
| Processing            | <span style="color:green">Yes</span>        | <span style="color:green">Yes</span> |
| Inspection            | <span style="color:green">Yes</span>        | <span style="color:green">Yes</span> |
| Can be extended       | <span style="color:orange">Good luck</span> | <span style="color:green">Yes</span> |
| Can accept anything   | <span style="color:red">No</span>           | <span style="color:green">Yes</span> |

Ok, let's review in detail why `Choixe` is better than `XConfig`:

### Being a python `Mapping`

The fact that `XConfig` is a python `Mapping` is a double-edged sword. On one hand, it's great because you automatically get all the methods you would expect from a plain python `dict`. On the other hand, what if you want to use `XConfig` with something that is not a `dict`, like a `Sequence`? You can't. And what if the result of your processing is something else than a `dict`? You have to use special methods like `unsafe_xxx` to achieve that, and that's not very pretty.

Furthermore, beside being a `Mapping`, `XConfig` is also a `Box`, which carries a lot of extra methods and hidden behaviors that make it extremely hard to extend. What if we want to create an `XConfig` that is a `Sequence`? That's pretty hard to do.

### Dot-Notation access

There is a big problem with dot-notation access in `Box`-like classes: you will need to clutter your code with `# type: ignore`s everywhere. That's because the dot-notation access is not a standard python feature, and it's not supported by most common type checkers and language servers. So, you reduce the amount of characters by 4 per access (from `my_obj.a` to `my_obj['a']`), but you have to manually hint the returned type and/or ignore potential errors. That's not very useful.

### Deep access

Deep access is cool, but its very coupled with the fact that `XConfig` implements a specific data structure. So, we decided to remove it from the new `Choixe` class. Besides, you can still perform deep access by using the `walk` method, or by simply using `pydash` functionalities.

### Schema validation

Nobody really uses choixe schema validation, because that feature is usually implemented in a more robust way in the application itself, or by means of external json-schema validators. So, we decided to remove it from the new `Choixe` class.


```{admonition} Note
:class: note

The `XConfig` class is still not deprecated, and since it's used pretty much everywhere in Pipelime, it will probably last for a little while. But, if you are starting a new project, we recommend you to use `Choixe` instead.
```