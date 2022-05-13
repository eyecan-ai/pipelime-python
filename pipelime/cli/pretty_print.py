import inspect
import typing as t

from pydantic import BaseModel
from pydantic.typing import display_as_type
from rich import box
from rich import print as rprint
from rich.table import Table

from pipelime.piper import PiperPortType


def _get_signature(model_cls: t.Type[BaseModel]) -> str:
    excluded = [
        mfield.alias
        for mfield in model_cls.__fields__.values()
        if mfield.field_info.exclude
    ]
    sig = inspect.signature(model_cls)
    sig = sig.replace(
        parameters=[p for p in sig.parameters.values() if p.name not in excluded],
        return_annotation=inspect.Signature.empty,
    )
    return str(sig)


def print_node_names(
    *model_cls: t.Type[BaseModel],
    show_class_path: bool = True,
):
    grid = Table.grid(padding=(0, 1))
    for m in model_cls:
        col_vals = [_command_title(m)]
        if show_class_path:
            col_vals.append(f"[italic grey50]{m.__module__}.{m.__name__}[/]")
        col_vals.append(inspect.getdoc(m))
        grid.add_row(*col_vals)
    rprint(grid)


def print_node_info(
    model_cls: t.Type[BaseModel],
    *,
    indent_offs: int = 2,
    show_class_path: bool = True,
):
    cpath_str = (
        f"\n[italic grey23]({model_cls.__module__}.{model_cls.__name__})[/]"
        if show_class_path
        else ""
    )

    grid = Table(
        "Fields",
        "Description",
        "Type",
        "Piper Port",
        "Required",
        "Default",
        box=box.SIMPLE_HEAVY,
        title=(
            f"[bold dark_red]{_command_title(model_cls)}[/]\n"
            f"[blue]{_get_signature(model_cls)}[/]{cpath_str}"
        ),
        caption=inspect.getdoc(model_cls),
        title_style="on white",
        expand=True,
    )

    for field in model_cls.__fields__.values():
        _field_row(grid, field, 0, indent_offs)

    rprint(grid)


def _command_title(model_cls: t.Type[BaseModel]) -> str:
    if model_cls.__config__.title:
        return model_cls.__config__.title
    return model_cls.__name__


def _field_row(grid: Table, field, indent: int, indent_offs: int):
    if field.field_info.exclude:
        return

    is_model = inspect.isclass(field.outer_type_) and issubclass(
        field.outer_type_, BaseModel
    )
    fname = field.name if not field.alias else field.alias
    fport = str(
        field.field_info.extra.get("piper_port", PiperPortType.PARAMETER).value
    ).upper()

    if fport == PiperPortType.INPUT.value.upper():
        fport = "[yellow]" + fport + "[/]"
    elif fport == PiperPortType.OUTPUT.value.upper():
        fport = "[cyan]" + fport + "[/]"

    line = [
        (" " * indent) + f"{fname}",
        "\u25B6 " + field.field_info.description
        if field.field_info.description
        else "",
        ("" if is_model else display_as_type(field.outer_type_).replace("[", r"\[")),
        fport,
        "[green]\u2713[/]" if field.required else "[red]\u2717[/]",
        f"{field.get_default()}",
    ]

    grid.add_row(*line)

    if is_model:
        for subfield in field.outer_type_.__fields__.values():  # type: ignore
            _field_row(grid, subfield, indent + indent_offs, indent_offs)
