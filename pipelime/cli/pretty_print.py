import inspect
import typing as t

from pydantic import BaseModel
from pydantic.typing import display_as_type
from rich import box
from rich import print as rprint
from rich.table import Table

from pipelime.piper import PiperPortType


def print_node_names(*model_cls: t.Type[BaseModel]):
    grid = Table.grid("Name", "Class Path", "Description", padding=(0, 1))
    for m in model_cls:
        grid.add_row(
            _command_title(m),
            f"[italic grey50]{m.__module__}.{m.__name__}[/]",
            inspect.getdoc(m),
        )
    rprint(grid)


def print_node_info(
    model_cls: t.Type[BaseModel],
    *,
    indent_offs: int = 2,
):
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
            f"[italic grey23]({model_cls.__module__}.{model_cls.__name__})[/]"
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
