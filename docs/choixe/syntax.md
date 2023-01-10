# Syntax

As I may have anticipated, **Choixe** features are enabled when a **directive** is found. **Directives** are special sub-structures that can appear in different forms:
- "compact"
- "call"
- "extended"
- "special"
- "key-value"

**Note**: some directives are available only in a subset of the previous forms.

## Compact Form

```yaml
$DIRECTIVE_NAME
```

Basically a `$` followed by a name. The name must follow the rules of python identifiers, so only alphanumeric characters and underscores ( `_` ), the name cannot start with a digit.

Examples:
-  `$index`
-  `$item`

Only directives without parameters can be expressed in the compact form.

## Call Form

```yaml
$DIRECTIVE_NAME(ARGS, KWARGS)
```

The call form extends the compact form with a pair of parenthesis containing the directive arguments. Arguments follow the rules of a plain python function call, in fact, they are parsed using the python interpreter.

Examples:
- `$var(x, default=hello, env=False)`
- `$for(object.list, x)`

The compact form is essentially a shortcut for the call form when no arguments are needed: `$model` is equivalent to `$model()`.

**Note**: due to some limitations of the current lexer, call forms can contain **at most** one set of parenthesis, meaning that you are **not** allowed to nest them like this:

- ~~`$directive(arg1=(1, 2, 3))`~~
- ~~`$directive(arg1="meow", arg2=$directive2(10, 20))`~~

If you really need to nest **directives**, you must use the **extended form**, introduced in the next paragraph.

## Extended Form

```yaml
$directive: DIRECTIVE_NAME
$args: LIST_OF_ARGS
$kwargs: DICT_OF_KWARGS
```

The extended form is a more verbose and more explicit alternative that allows to pass complex arguments that cannot be expressed with the current limitations of the call form.

Examples:
- ```yaml
  $directive: var
  $args:
    - x
  $kwargs:
    default: hello
    env: false
  ```
- ```yaml
  $directive: sweep
  $args:
    - 10
    - [10, 20]
    - a: $var(x, default=30) # Directive nesting
      b: 60
  $kwargs: {}
  ```

## Special Form

Some **directives** are available only with special forms, i.e. some forms that do not have a schema, and depend from the specific **directive** used. Do not worry, they are just a few, they are detailed below and their schema is easy to remember.

Example:
```yaml
$model: package.MySuperCoolPydanticModel
$args:
  foo: 10
  bar: 20
```

Here, the whole dictionary is recognized as a special form and parsed as a single block.

## Key-Value Form

When a directive is expressed in the key-value form, the key-value pair containing the directive is parsed as a single block, instead of having two separate parsings for the key and the value, as occurs normally.

Key-value forms are a lot like special forms, the only difference is that the parsing, instead of consuming the whole dictionary, it consumes only the key-value pair containing the directive.

Example:
```yaml
$for(my_list): hello
```

Here, we have a dictionary with a single entry consisting of a key and value pair. The directive `$for` is recognized as a key-value form and thus, instead of parsing the key and the value separately, they are parsed as a single block. Contrary to the special and extended form, the dictioanry containing the pair is not consumed by the parsing operation.

## String Bundles

**Directives** in compact or call form can also be mixed with plain strings, creating a "String Bundle":

`$var(animal.name) is a $var(animal.species) and their owner is $var(animal.owner, default="unknown")`

In this case, the string is tokenized into 5 parts:
1. `$var(animal.name)`
2. ` is a `
3. `$var(animal.species)`
4. ` and their owner is `
5. `$var(animal.owner, default="unknown")`

The result of the computation is the string concatenation of the result of each individual token: `Oliver is a cat and their owner is Alice`.


## Dict Bundles
A "Dict Bundle" is analogous to a "String Bundle" but with:
- Dictionary union instead of string concatenation
- Key-Value forms instead of compact/call forms

Just like Compact/Call forms can be mixed with plain strings, Key-Value forms can be mixed with plain key-value pairs in a dictionary:

```yaml
name: mario
$for(my_list, x): hello
age: 203
$for(my_list2, y):
  - $item(y.foo)
```

## Directives table

| Directive | Compact | Call  | Extended | Special | Key-Value |
| :-------: | :-----: | :---: | :------: | :-----: | :-------: |
|   `var`   |    ❌    |   ✔️   |    ✔️     |    ❌    |     ❌     |
| `import`  |    ❌    |   ✔️   |    ✔️     |    ❌    |     ❌     |
|  `sweep`  |    ❌    |   ✔️   |    ✔️     |    ❌    |     ❌     |
| `symbol`  |    ❌    |   ✔️   |    ✔️     |    ❌    |     ❌     |
|  `call`   |    ❌    |   ❌   |    ❌     |    ✔️    |     ❌     |
|  `model`  |    ❌    |   ❌   |    ❌     |    ✔️    |     ❌     |
|   `for`   |    ❌    |   ❌   |    ❌     |    ❌    |     ✔️     |
| `switch`  |    ❌    |   ❌   |    ❌     |    ❌    |     ✔️     |
|  `item`   |    ✔️    |   ✔️   |    ✔️     |    ❌    |     ❌     |
|  `index`  |    ✔️    |   ✔️   |    ✔️     |    ❌    |     ❌     |
|  `uuid`   |    ✔️    |   ✔️   |    ✔️     |    ❌    |     ❌     |
|  `date`   |    ✔️    |   ✔️   |    ✔️     |    ❌    |     ❌     |
|   `cmd`   |    ❌    |   ✔️   |    ✔️     |    ❌    |     ❌     |
| `tmp_dir` |    ✔️    |   ✔️   |    ✔️     |    ❌    |     ❌     |
|  `rand`   |    ✔️    |   ✔️   |    ✔️     |    ❌    |     ❌     |
