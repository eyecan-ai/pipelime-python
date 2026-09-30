# S3 — CLI help, TUI, error display, `main.py` (full suite green)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the CLI help renderer, the TUI and the run path from pydantic v1 `ModelField` introspection to the toolkit's `FieldView`, with modern type hints rendering correctly, and bring the **whole test suite** back to green.

**Architecture:** Every place that read `__fields__`/`outer_type_`/`field_info`/`__config__` now consumes `FieldView`s from `iter_fields()`/`get_field()`; `_human_readable_type` is rebuilt on `type_info()`; `cli/main.py` prints `format_validation_error(e, cmd_cls)` before re-raising. Rendering is pinned by the help snapshot contract test.

**Tech Stack:** pydantic 2.x, rich, textual.

**Spec:** `docs/superpowers/specs/2026-09-12-pydantic-v2-migration-design.md` §4.6, §4.8.

## Global Constraints

- Rendered help for existing (typing-style) annotations must match the snapshot `tests/sample_data/contract/help_contract_ports.txt` byte for byte after whitespace collapsing; modern hints render as: `list[int]` → `[int, ...]`, `dict[str, float]` → `{str: float}`, `tuple[int, str]` → `(int, str)`, `int | str` → `int | str`, `X | None` → `X` (like `Optional[X]`).
- TUI `get_field_type()` keeps its exact strings for `typing` annotations (`List[int]`, `Optional[int]`/`Union[int, NoneType]`, `Dict[str, Any]`, `Union[...]`, `Tuple[...]`) and renders modern hints as written (`list[int]`, `int | None`).
- Same mechanical rules as S2a/S2b.

---

### Task S3-T1: `cli/pretty_print.py`

**Files:**
- Modify: `pipelime/cli/pretty_print.py`

- [ ] **Step 1: Imports and titles**

```python
from pydantic import BaseModel

from pipelime.utils.pydantic_compat import FieldView, iter_fields, model_title, type_info
```
```python
def get_model_title(model_cls: t.Type[BaseModel]) -> str:
    return model_title(model_cls)
```

- [ ] **Step 2: Field value printing**

```python
def print_model_field_values(
    model_fields: t.Mapping[str, FieldView],
    port_values: t.Mapping[str, t.Any],
    icon: str = "",
):
    for k, v in port_values.items():
        rprint(f"\n{icon if icon else '***'} {k}:")
        # Ports might be virtual, as in ShellCommand, so they might not be in the model
        if k in model_fields and model_fields[k].description:
            rprint(f"[italic grey50]{escape(model_fields[k].description)}[/]")
        rprint(
            "[green]"
            + escape(str(v) if isinstance(v, (bytes, str)) else repr(v))
            + "[/]"
        )


def _fields_by_name(model_cls) -> t.Dict[str, FieldView]:
    return {f.name: f for f in iter_fields(model_cls)}


def print_command_inputs(command: "PipelimeCommand"):  # type: ignore # noqa: E602,F821
    print_model_field_values(_fields_by_name(type(command)), command.get_inputs(), _input_icon())


def print_command_outputs(command: "PipelimeCommand"):  # type: ignore # noqa: E602,F821
    print_model_field_values(_fields_by_name(type(command)), command.get_outputs(), _output_icon())
```

- [ ] **Step 3: `_field_row`** (signature unchanged; `field` is now a `FieldView`)

```python
def _field_row(
    grid: Table,
    field: FieldView,
    indent: int,
    indent_offs: int,
    show_piper_port: bool,
    show_description: bool,
    recursive: bool,
):
    from enum import Enum

    expand_help = field.extra.get("expand_help", False)

    inner_type = field.inner_type
    is_model = field.is_model and not inspect.isabstract(inner_type)

    if show_description:
        # NB: docs should not come from the inner root type
        if field.description:
            field_docs = field.description
        elif hasattr(inner_type, "__doc__") and inner_type.__doc__:
            field_docs = str(inspect.getdoc(inner_type))
        else:
            field_docs = ""

    root_type = field.root_type if is_model else None
    has_root_item = root_type is not None
    field_outer_type = root_type if has_root_item else inner_type

    if show_piper_port:
        from pipelime.piper import PiperPortType

        fport = str(
            field.extra.get("piper_port", PiperPortType.PARAMETER).value
        ).upper()

        if fport == PiperPortType.INPUT.value.upper():
            fport = f"{_input_icon()} [yellow]{fport}[/]"
        elif fport == PiperPortType.OUTPUT.value.upper():
            fport = f"{_output_icon()} [cyan]{fport}[/]"
        else:
            fport = f"{_parameter_icon()} {fport}"

    # Field name & alias
    line = [
        (" " * indent)
        + ("[bold dark_orange]" if indent == 0 else "")
        + (
            f"{escape(field.name)} / "
            if field.populate_by_name and field.has_alias
            else ""
        )
        + f"{escape(field.effective_alias)}"
        + ("[/]" if indent == 0 else "")
    ]

    # Description
    if show_description:
        line.append("▶ " + escape(field_docs))  # type: ignore

    # Type
    line.append(
        ""
        if is_model and not has_root_item and (recursive or expand_help)
        else escape(_human_readable_type(field_outer_type))
    )

    # Piper port
    if show_piper_port:
        line.append(fport)  # type: ignore

    # Default value
    if field.required:
        line.append("[red]✗[/]")
    else:
        field_default = field.default
        if isinstance(field_default, Enum):
            field_default = field_default.value
        line.append(f"[green]{field_default}[/]")

    grid.add_row(*line)

    # ... the `if recursive or expand_help:` block is unchanged ...
```

- [ ] **Step 4: `_get_signature`, `_human_readable_type`, `_iterate_model_fields`**

In `_get_signature`:
```python
    fields = list(iter_fields(model_cls))
    fullname = {f.effective_alias: f.name for f in fields}
    excluded = [f.effective_alias for f in fields if f.exclude]
```
(the rest unchanged: `inspect.signature(model_cls)` still drives it.)

```python
def _human_readable_type(field_outer_type):
    from enum import Enum

    v = field_outer_type
    ti = type_info(v)

    if ti.is_union:
        return " | ".join(map(_human_readable_type, ti.args))
    if inspect.isclass(ti.origin):
        if issubclass(dict, ti.origin):
            return "{" + ": ".join(map(_human_readable_type, ti.args)) + "}"
        if issubclass(list, ti.origin):
            return "[" + ", ".join(map(_human_readable_type, ti.args)) + ", ...]"
        if issubclass(tuple, ti.origin):
            return "(" + ", ".join(map(_human_readable_type, ti.args)) + ")"

    if inspect.isclass(v) and issubclass(v, Enum):
        v = v.__name__ + "{" + ", ".join(e.name.lower() for e in v) + "}"
    elif ti.origin is not None or not inspect.isclass(v):
        # generic aliases (`Sequence[int]`, `Literal[...]`) and typing objects
        v = str(v).replace("typing.", "")
    else:
        v = v.__name__

    return v.replace("NoneType", "None")
```

```python
def _iterate_model_fields(model_cls, grid, indent, indent_offs, show_piper_port, show_description, recursive, add_blank_row):
    no_data = True
    for field in iter_fields(model_cls):
        if not field.exclude:
            no_data = False
            _field_row(grid, field, indent=indent, indent_offs=indent_offs, show_piper_port=show_piper_port, show_description=show_description, recursive=recursive)
            if add_blank_row:
                grid.add_row()
    if no_data:
        grid.add_row((" " * indent) + "[grey50 italic]([strike]no parameters[/])[/]")
```
Delete the `pydantic.v1.typing` import block and `_recursive_args_flattening`/`_get_inner_args` stay as they are (they use `t.get_args`, fine for both spellings).

- [ ] **Step 5: Check against the snapshot and commit**

Run: `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/test_pydantic_contract.py -k "HelpRendering or test_help"`
Expected: 2 passed. If the snapshot differs, diff the two whitespace-collapsed strings: the *only* acceptable difference is none — fix the renderer, not the snapshot.

```bash
git add pipelime/cli/pretty_print.py
git commit -m "refactor(cli): help rendering on FieldView/type_info with modern hints"
```

---

### Task S3-T2: TUI — `cli/tui/utils.py`, `cli/tui/tui.py`

**Files:**
- Modify: `pipelime/cli/tui/utils.py`, `pipelime/cli/tui/tui.py`

- [ ] **Step 1: `tui/utils.py`**

Imports:
```python
import inspect
from enum import Enum
from typing import List, Mapping, Tuple, Type, cast

from pydantic import BaseModel

from pipelime.cli.utils import PipelimeSymbolsHelper
from pipelime.piper import PipelimeCommand
from pipelime.stages import SampleStage, StageInput
from pipelime.utils.pydantic_compat import FieldView, iter_fields
```
`TuiField(BaseModel)` unchanged.

`is_tui_needed`:
```python
    for field in iter_fields(cmd_cls):
        name = field.name
        alias = field.effective_alias
        required = field.required
        if (name not in cmd_args) and (alias not in cmd_args) and required:
            return True
        if field.inner_type is StageInput:
            ...  # unchanged body (uses name/alias)
```

`init_tui_field(field: FieldView, args: Mapping) -> TuiField`:
```python
    default = ""
    hint = ""
    if field.name in args:
        default = str(args[field.name])
    elif field.effective_alias in args:
        default = str(args[field.effective_alias])
    else:
        field_default = None if field.required else field.default
        if field_default is not None:
            if isinstance(field_default, BaseModel):
                hint = str(field_default)
            else:
                if isinstance(field_default, Enum):
                    field_default = field_default.value
                default = str(field_default)

    return TuiField(
        simple=True,
        name=field.name,
        description=str(field.description),
        hint=hint,
        type_=get_field_type(field),
        value=default,
    )
```

`init_stageinput_tui_field(field: FieldView, cmd_args)`: `field.name`/`field.alias` → `field.name`/`field.effective_alias`; `for field in stage_cls.__fields__.values()` → `for f in iter_fields(stage_cls): tui_fields.append(init_tui_field(f, stage_args))`.

`get_field_type(field: FieldView) -> str`:
```python
    type_ = field.annotation
    if inspect.isclass(type_) and not getattr(type_, "__args__", None):
        type_ = type_.__name__
    else:
        # `typing` aliases print as `typing.List[int]`, modern hints as written
        # (`list[int]`, `int | None`); never rely on `__name__` (UnionType has none)
        type_ = str(type_).replace("typing.", "")
    # replace common pipelime types
    ...  # unchanged mapping loop
    return type_
```

- [ ] **Step 2: `tui/tui.py`**

```python
from pipelime.utils.pydantic_compat import iter_fields
...
    def init_fields(self, cmd_args: Mapping) -> Dict[str, TuiField]:
        tui_fields = {}
        for field in iter_fields(self.cmd_cls):
            if field.inner_type is StageInput:
                tui_fields[field.name] = init_stageinput_tui_field(field, cmd_args)
            else:
                tui_fields[field.name] = init_tui_field(field, cmd_args)
        return tui_fields
```
(line 189 uses the `TuiField.type_` string — unchanged.)

- [ ] **Step 3: Test edits (v1 forms) in `tests/pipelime/cli/test_tui.py`** (record in `tests/TEST_CHANGES.md`)

- `from pydantic.v1 import BaseModel, Field` → `from pydantic import BaseModel` + `from pipelime.piper import Field`.
- add `from pipelime.utils.pydantic_compat import get_field, iter_fields`.
- `X.__fields__["name"]` → `get_field(X, "name")` (all occurrences, including inside `init_tui_field(...)` calls).
- `X.__fields__["name"].field_info.description` → `get_field(X, "name").description`.
- `X.__fields__["grabber"].get_default()` → `get_field(X, "grabber").default`.
- `fields = ModelWithComplexFieldsTypes.__fields__` → `fields = {f.name: f for f in iter_fields(ModelWithComplexFieldsTypes)}`.

- [ ] **Step 4: Run the TUI tests and the modern-hints contract**

Run: `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/cli/test_tui.py tests/pipelime/test_pydantic_contract.py -k "tui or TUI or ModernTypeHints"`
Expected: passed (the `test_tui` contract xfail now passes).

```bash
git add pipelime/cli/tui tests/pipelime/cli/test_tui.py tests/TEST_CHANGES.md
git commit -m "refactor(cli): TUI on FieldView, modern type hints supported"
```

---

### Task S3-T3: `cli/main.py` and the rest of `cli/utils.py`

**Files:**
- Modify: `pipelime/cli/main.py`, `pipelime/cli/utils.py`
- Modify (tests): `tests/pipelime/cli/test_base.py`

- [ ] **Step 1: `main.py`**

- `from pydantic.v1 import BaseModel` → `from pydantic import BaseModel` (`PlCliOptions` stays a plain model).
- `return self._purge(self.dict())` → `return self._purge(self.model_dump())`.
- In the run function: `from pydantic.v1.error_wrappers import ValidationError` → `from pydantic import ValidationError`; import `format_validation_error` instead of `show_field_alias_valerr` from `pipelime.cli.utils` and `print_error` from `pipelime.cli.pretty_print`;
```python
    except ValidationError as e:
        print_error(format_validation_error(e, cmd_cls))
        raise e
```
- `print_info(cmd_obj.dict(), pretty=True)` → `print_info(cmd_obj.model_dump(), pretty=True)`.

- [ ] **Step 2: `cli/utils.py`** — remove the `show_field_alias_valerr` alias added in S2b-T7 if nothing imports it any more (`grep -rn show_field_alias_valerr pipelime tests`); keep `format_validation_error`.

- [ ] **Step 3: Test edits in `tests/pipelime/cli/test_base.py`** (v1 forms; record them)

- `from pydantic.v1 import Field` → `from pipelime.piper import Field`.
- both local `from pydantic.v1 import ValidationError` → `from pydantic import ValidationError`.
- The contract test `test_alias_error_formatting` already covers the alias text.

- [ ] **Step 4: Leftover grep**

Run: `grep -rn "pydantic.v1" pipelime | grep -v "pydantic_compat.py\|choixe/ast/nodes.py"`
Expected: no matches.

- [ ] **Step 5: Commit**

```bash
git add pipelime/cli tests/pipelime/cli/test_base.py tests/TEST_CHANGES.md
git commit -m "refactor(cli): run path and options on pydantic v2"
```

---

### Task S3-T4: Full suite gate (Tier 2)

- [ ] **Step 1: Run everything**

Run: `make test-full 2>&1 | tail -15`
Expected: `2406 + <new contract/toolkit tests> passed` with the same skips as the S0 baseline, **no failures, no xfails left** (all three S0 xfails now pass because `V1` is false). Then `make test-tier1` for a serial confirmation of the CLI slice.

- [ ] **Step 2: Triage rule** for any failure: read the test; if it pins v1 behaviour, fix the source; if it uses a v1 API form in test-local code, edit it and add a `TEST_CHANGES.md` row; never weaken an assertion.

- [ ] **Step 3: Ledger**

Record the S3 gate (command, counts, wall time, commit). Then open `2026-09-13-pydantic-v2-s4-choixe.md`.
