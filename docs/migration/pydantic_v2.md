# Migrating to pipelime 3 (pydantic v2)

pipelime 3.0 is built on the native **pydantic v2** API. pipelime 2.x used the
`pydantic.v1` compatibility layer; pydantic 3 removes it, so every project that
defines commands, stages, sequences or entities must move too. pipelime keeps the
behaviour you rely on — for most projects the import changes of section 1 (and the
validators of section 2, if you have any) are all that is needed.

pipelime 3 requires `pydantic>=2.10,<3` and `pydantic-extra-types`.

## 1. Change the imports (required)

```python
# before
import pydantic.v1 as pyd
from pydantic.v1 import Field, PrivateAttr, validator

# after
import pydantic as pyd
from pydantic import PrivateAttr, field_validator
from pipelime.piper import Field  # pydantic.Field + pipelime flags (piper_port, ...)
```

`pipelime.piper.Field` accepts everything `pydantic.Field` accepts plus the pipelime
flags `piper_port=`, `pipe_source=`, `expand_help=`, `is_required=` (and the v1
`regex=`, translated to `pattern=`). A plain `pydantic.Field(..., piper_port=...)`
still works — pipelime finds the flag — but pydantic emits a
`PydanticDeprecatedSince20` warning for each `Field(...)` call using extra keywords, so
prefer the pipelime `Field`.

An `Optional[X]` field whose `Field(...)` gives no default, e.g.
`x: Optional[int] = Field(description="...")`, stays optional (default `None`) with
`pipelime.piper.Field`, as in 2.x. With a raw `pydantic.Field`, pass `None` explicitly
(`pydantic.Field(None, description="...")`) or use `pipelime.piper.Field`: pydantic
cannot tell `pydantic.Field(description=...)` apart from `pydantic.Field(...)`, so the
field would be required. `Field(...)` is required with either `Field`, as in 2.x.

The v1 constraint names of `Field` are not all accepted by pydantic v2, with either
`Field`:

| v1 `Field(...)` keyword | pipelime 3 |
|---|---|
| `min_items=n`, `max_items=n` | converted by pydantic to `min_length=n`/`max_length=n`, with a `PydanticDeprecatedSince20` warning; write `min_length`/`max_length` |
| `unique_items=True` | `PydanticUserError` when the class is defined; use a `set[X]` field (or a validator) |
| `const=value` | `PydanticUserError` when the class is defined; annotate the field as `Literal[value]` |
| `regex=...` | translated to `pattern=` by `pipelime.piper.Field` (pydantic's own `Field` rejects it) |

If a subclass of a pipelime model still contains a `pydantic.v1` `Field`,
`PrivateAttr`, `@validator` or `@root_validator`, pipelime raises a `TypeError`
naming the attribute when the class is defined (i.e. at import time):

```text
TypeError: `MyCommand.a` is a `pydantic.v1` object (FieldInfo). pipelime 3 is built on
native pydantic v2: replace `import pydantic.v1` with `import pydantic`, ...
```

Without this check a v1 `Field` on a v2 model would silently become the field's
default *value* (a `FieldInfo` object).

Code that catches validation errors must catch `pydantic.ValidationError`: the errors
raised by pipelime 3 are not `pydantic.v1.ValidationError` instances (both are
`ValueError` subclasses).

## 2. Validators

| pipelime 2.x (`pydantic.v1`) | pipelime 3 (pydantic v2) |
|---|---|
| `@validator("x")` | `@field_validator("x")` + `@classmethod` |
| `@validator("x", pre=True)` | `@field_validator("x", mode="before")` + `@classmethod` |
| `@validator("x", always=True)` | `@field_validator("x")` and `x: T = Field(<default>, validate_default=True)` |
| `@validator("*")` | `@field_validator("*")` + `@classmethod` |
| `@validator("x", each_item=True)` | no direct equivalent: validate the items through the item type, e.g. `x: list[Annotated[int, AfterValidator(f)]]`, or loop over the items inside a `@field_validator("x")` (the deprecated `pydantic.validator("x", each_item=True)` still works, with a warning) |
| `def check(cls, v, values)` | `def check(cls, v, info: pydantic.ValidationInfo)` and `info.data` |
| `@root_validator` | `@model_validator(mode="after")` on an instance method returning `self` |
| `@root_validator(pre=True)` | `@model_validator(mode="before")` + `@classmethod` (receives the raw input) |

```python
from pydantic import ValidationInfo, field_validator, model_validator

from pipelime.piper import Field, PipelimeCommand


class ScaleCommand(PipelimeCommand, title="scale-example"):
    """Scale a value."""

    factor: int = 1
    value: int = Field(None, validate_default=True)  # 2.x: @validator(..., always=True)

    @field_validator("value")
    @classmethod
    def _default_value(cls, v, info: ValidationInfo):
        # `info.data` holds the fields validated so far (2.x: `values`)
        return info.data["factor"] * 10 if v is None else v

    @model_validator(mode="after")
    def _check(self):
        if self.value < 0:
            raise ValueError("value must be non-negative")
        return self

    def run(self):
        print(self.value * self.factor)
```

Without `validate_default=True` a `@field_validator` does not run on the default value
(as `@validator` without `always=True` did not).

pydantic v2 still ships `@validator` and `@root_validator` as deprecated aliases, so
old validators keep working (including `values` and `always=True`) with a
`PydanticDeprecatedSince20` warning; the deprecated `@root_validator` also requires
`skip_on_failure=True` unless `pre=True`, otherwise pydantic raises a
`PydanticUserError` when the class is defined. They must come from `pydantic`, not
from `pydantic.v1` (see the check above).

## 3. Other pydantic API renames

These methods are pydantic's own renames. The old names keep working on pipelime
models, each with a `PydanticDeprecatedSince20` warning, so they can be migrated at
your own pace:

| pydantic v1 | pydantic v2 |
|---|---|
| `m.dict()`, `m.json()`, `m.copy()` | `m.model_dump()`, `m.model_dump_json()`, `m.model_copy()` |
| `M.parse_obj(d)`, `M.parse_raw(s)` | `M.model_validate(d)`, `M.model_validate_json(s)` |
| `M.schema()` | `M.model_json_schema()` |

**`__fields__` must be migrated.** It still resolves (with a warning), but its values
are v2 `FieldInfo` objects, not v1 `ModelField`s: `.field_info`, `.type_`,
`.outer_type_` and `.required` raise `AttributeError`. Use instead:

- `M.model_fields` — name → `FieldInfo` (`.annotation`, `.alias`, `.default`,
  `.description`, `.is_required()`, `.json_schema_extra`, ...);
- `pipelime.utils.pydantic_compat.iter_fields(M)` (an iterator) and
  `get_field(M, name)` — they return a `FieldView` with `.name`, `.alias`,
  `.annotation`, `.required`, `.description`, `.default` and `.extra` (the pipelime
  flags, e.g. `.extra["piper_port"]`, which 2.x code read from
  `__fields__[name].field_info.extra`);
- `pipelime.utils.pydantic_compat.field_extra(M.model_fields[name], "piper_port")` to
  read one flag from a `FieldInfo`.

**`class Config`** is still accepted (pydantic warns that class-based config is
deprecated; `model_config = pydantic.ConfigDict(...)` is the v2 spelling). pydantic v2
renamed several keys and, on a plain `pydantic.BaseModel`, *ignores* the v1 names with
just a warning. On pipelime models (and in the class keywords, e.g.
`class M(PipelimeModel, anystr_strip_whitespace=True)`) pipelime translates them, so
the setting keeps applying:

| v1 key | v2 key |
|---|---|
| `allow_population_by_field_name` | `populate_by_name` |
| `anystr_strip_whitespace`, `anystr_lower`, `anystr_upper` | `str_strip_whitespace`, `str_to_lower`, `str_to_upper` |
| `min_anystr_length`, `max_anystr_length` | `str_min_length`, `str_max_length` |
| `orm_mode` | `from_attributes` |
| `schema_extra` | `json_schema_extra` |
| `validate_all` | `validate_default` |
| `keep_untouched` | `ignored_types` |
| `allow_mutation = False` | `frozen = True` (inverted) |

When both spellings of a setting are given, the nearest definition wins, as for any
attribute: a `class Config(Parent.Config)` (or `Config(SomeBase)`) overrides what it
inherits whichever spelling either uses; within the same class — or among the class
keywords — the v2 key wins.

The v1 keys removed in v2 have no equivalent and no effect —
`fields` (use `Field(alias=...)` on the field), `error_msg_templates`, `getter_dict`,
`json_loads`, `json_dumps`, `copy_on_model_validation`, `post_init_call`, and
`smart_union` / `underscore_attrs_are_private` (v2 always behaves as if they were
`True`). In a `class Config` pydantic warns about them ("... has been removed"); given
as class keywords (e.g. `class S(SampleStage, copy_on_model_validation="none")`, the
2.x idiom), pipelime drops them with a `UserWarning` naming the key (pydantic alone
would raise `TypeError: S.__init_subclass__() takes no keyword arguments`). Remove
them when convenient.

## 4. What stays the same

pipelime restores the pydantic v1 behaviour below on its own models — every subclass
of `PipelimeCommand`, `SampleStage`, `SamplesSequence`, `BaseEntity`, `EntityAction`
and of `pipelime.utils.pydantic_compat.PipelimeModel` (derive your own helper models from
`PipelimeModel` to get the same rules; a plain `pydantic.BaseModel` follows plain
pydantic v2 rules).

- `Optional[X]` / `X | None` fields without a default are optional (default `None`),
  including `x: Optional[X] = Field(description=...)`; `x: T = None` accepts an explicit
  `None` (the annotation becomes `Optional[T]`). With a raw `pydantic.Field`, pass `None`
  explicitly or use `pipelime.piper.Field` (section 1).
- Numbers and bools are coerced into `str` fields (`1` → `"1"`, `True` → `"True"`,
  e.g. for CLI values such as `+name true`). `StrictStr` and `Field(strict=True)` still
  reject them. A model-level `ConfigDict(strict=True)` rejects numbers but — unlike
  pydantic v2 — still turns a bool into `"True"`; use `StrictStr` to reject bools.
- `Path` fields reject bools, as in 2.x.
- The v1 key names of a `class Config` (or of the class keywords) keep applying:
  pipelime translates them to the v2 names (see section 3).
- Value wrappers (`NumpyType`, `YamlInput`, `TypeDef`/`ItemType`, `CallableDef`,
  `StageInput`, ...): `NumpyType(__root__=...)`, `.__root__`, `.value`, `.create()`,
  `.validate()` and the `{"__root__": ...}` shape of `.dict()` (`model_dump()` returns
  the bare value, as in pydantic v2). `StageInput.dict()` keeps its 2.x
  `{"<stage title>": {<args>}}` shape.
- Compact forms (`"folder,true"`, `"4,2"`, `"0.3,out"`), `Field(piper_port=...)`
  discovery, polymorphic dumps of stages/commands held by fields typed as their base
  class, DAG and pipe configs written by pipelime 2.x (including
  `entity: {__root__: ...}`).
- `pipelime help` and DAG validation errors show `name / alias` for aliased fields.
- `pipelime.cli.utils.show_field_alias_valerr` is kept as an alias of the new
  `format_validation_error(e, model_cls=None)`. Note that it now *returns* the
  formatted text (with `name / alias` locations when `model_cls` is given) and leaves
  `e` untouched; in 2.x it rewrote the locations of `e` in place and returned `None`.

## 5. New

- Modern type hints (`list[int]`, `dict[str, X]`, `tuple[int, str]`, `X | None`,
  `X | Y`) work everywhere in commands, stages, sequences and entities, including
  `pipelime help` and the TUI (the 2.x TUI failed on `X | Y` fields and showed
  `list[int]` as `list`).
- `SamplesSequence.to_pipe()` works on pipes with string fields (2.x raised a
  `RecursionError`, e.g. on `.enumerate(idx_key="i")`).
- `@command` functions with `**kwargs` receive the extra keyword arguments expanded
  (for `def f(a, **kw)`, 2.x called `f(a, kw={"x": 2})` instead of `f(a, x=2)`).
- An unannotated `@command` parameter with a `None` default is accepted (typed
  `Any`); 2.x raised a `ConfigError` ("unable to infer type") at decoration.

## 6. Behaviour differences you may notice

- `M.__fields__` holds v2 `FieldInfo` objects: v1 `ModelField` attributes
  (`.field_info`, `.type_`, `.outer_type_`, `.required`) raise `AttributeError` — use
  `model_fields` or `iter_fields`/`get_field` (section 3).
- **Validation is pydantic v2's.**
  - Unions are resolved in "smart" mode instead of left-to-right: an input that
    exactly matches a member keeps that member's type. `int | str` given `"5"` now
    yields `"5"` (2.x: `5`); `Union[str, int]` given `5` yields `5` (2.x: `"5"`);
    `Union[int, float]` given `1.5` yields `1.5` (2.x: `1`). Check the unions of your
    models whose members overlap.
  - A few lax v1 coercions are gone: an `int` field rejects a float with a fractional
    part (`1.5`; 2.x truncated it to `1`), while `1.0` and `"1"` are still accepted.
  - Error messages have the v2 format (`Input should be a valid integer ...
    [type=int_parsing, ...]` instead of `value is not a valid integer
    (type=type_error.integer)`), and `e.errors()` uses the v2 error types.
- An explicit `None` passed to a *required* value-wrapper field (e.g.
  `pipe: YamlInput = Field(...)` given `None`) was rejected by 2.x ("none is not an
  allowed value"); it is now accepted as a wrapper holding `None` (the same result as
  `YamlInput.create(None)`).
- The dump of an `entity` stage has no `__root__` envelope any more:
  `{"entity": {"action": ..., "input_type": ...}}` (2.x `.dict()`:
  `{"entity": {"__root__": {...}}}`). Both shapes are accepted as input.
- `NumpyType.create(arr)` and `NumpyType.validate(arr)` keep the given array by
  identity (2.x copied it on these paths; `NumpyType(__root__=arr)` already kept it).
- `pipelime help` and the TUI show constrained types by their base type:
  `PositiveInt` → `int`, `bool | PositiveInt` → `bool | int` (the v2 constrained types
  are `Annotated[int, Gt(gt=0)]`; the help's signature line prints them that way). The
  TUI shows `x: T = None` as `Optional[T]` (2.x: `T`).
- **Choixe**
  - `$model` requires a pydantic v2 model; a `pydantic.v1` model raises a `TypeError`
    pointing to this guide.
  - A token directive given a wrong directive as argument (e.g.
    `$import("$symbol(builtins.str)")`, `$var("$import('x.json')")`) now raises
    `ChoixeParsingError` ("... does not validate against any of the available special
    or extended forms"); 2.x leaked a raw `pydantic.v1.ValidationError` for these
    token forms. Callers that caught `ValidationError` (or `ValueError`) around
    `pipelime.choixe.ast.parser.parse` must catch `ChoixeParsingError`, which is not a
    `ValueError`.
  - AST nodes are standard dataclasses: constructing a node directly with a
    wrong-typed argument raises `TypeError` (2.x: `pydantic.v1.ValidationError`), and
    tuples/dicts are no longer converted into nodes (2.x:
    `ForNode(iterable=("x",), ...)` built a `LiteralNode("x")`); pass node instances.
