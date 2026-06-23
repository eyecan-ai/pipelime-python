# Migration notes: Pydantic v1 → v2

pipelime now uses the **native Pydantic v2 API** internally (previously it relied
on the `pydantic.v1` compatibility layer). The dependency is `pydantic>=2,<3`.
If you subclass pipelime models, parse/serialize them, or read their JSON schema,
note the following user-facing changes.

## Method renames on pipelime models

| v1 | v2 |
|----|----|
| `model.dict(...)` | `model.model_dump(...)` |
| `model.json(...)` | `model.model_dump_json(...)` |
| `Model.parse_obj(x)` | `Model.model_validate(x)` |
| `Model.parse_raw(s)` | `Model.model_validate_json(s)` |
| `pydantic.parse_obj_as(T, x)` | `pydantic.TypeAdapter(T).validate_python(x)` |
| `Model.__fields__` | `Model.model_fields` |
| `Model.__config__.title` | `Model.model_config.get("title")` |

## Root models serialize their value directly

The custom root types — `NumpyType`, `YamlInput`, `TypeDef`/`ItemType`,
`CallableDef`, `StageInput`, `NodesDefinition` and the entity/transformation
wrappers — are now `pydantic.RootModel` subclasses:

- Construct them positionally: `NumpyType(arr)` instead of `NumpyType(__root__=arr)`.
- Access the wrapped value via `.root` (or the existing `.value` property) instead
  of `.__root__`.
- `model_dump()` returns the serialized value **directly**; there is no longer a
  `{"__root__": ...}` envelope. Callers that did `x.dict()["__root__"]` should use
  `x.model_dump()`.

## `Optional[X]` fields now require an explicit default

In Pydantic v2 `Optional[X]` no longer implies `= None`. Entity/command/stage
fields that should be optional must now declare a default explicitly:

```python
# v1 (optional implicitly defaulted to None)
label: Optional[NumpyItem]
# v2
label: Optional[NumpyItem] = None
```

Without the default the field becomes **required**.

## Stricter validation

Pydantic v2 is stricter about type coercion and raises `pydantic.ValidationError`
(with v2-formatted messages) where v1 may have silently coerced. Error messages
and `model_json_schema()` output follow the v2 format.

## Config keys

Custom `model_config` / class-keyword config changed:

- `allow_population_by_field_name` → `populate_by_name`
- `allow_mutation=False` → `frozen=True`
- `copy_on_model_validation` → removed (no effect in v2)
- `underscore_attrs_are_private` → removed (automatic in v2)
- `extra=pydantic.Extra.forbid` → `extra="forbid"` (string literals)

## Other changes

- The padding color type in `StageCropAndPad` now comes from
  `pydantic_extra_types.color.Color` (the successor of `pydantic.v1.color.Color`),
  added as a dependency. Its public API (`Color("name")`, `.as_rgb_tuple(...)`) is
  unchanged.
- DAG node validation errors no longer expand field aliases to `name / alias` in
  the reported error locations: in v2 the originating model class is not attached
  to the `ValidationError`, so the raw v2 locations are shown.
