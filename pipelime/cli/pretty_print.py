import inspect
import typing as t

from pydantic import BaseModel
from pydantic.typing import display_as_type
from rich import box
from rich.markup import escape
from rich import get_console
from rich import print as rprint
from rich.pretty import Pretty
from rich.table import Table, Column
from rich.text import Text
from rich.style import Style
from rich.panel import Panel

if t.TYPE_CHECKING:
    from pipelime.cli.utils import ActionInfo


def _input_icon():
    return "📥"


def _output_icon():
    return "📦"


def _parameter_icon():
    return "📐"


def _short_line():
    return "━━━━━"


def print_with_style(
    val,
    style: t.Optional[str] = None,
    *,
    pretty: bool = False,
    end: str = "\n",
    indent_guides: bool = True,
):
    get_console().print(
        Pretty(val, indent_guides=indent_guides, expand_all=True) if pretty else val,
        style=style,
        end=end,
    )


def print_debug(
    val, *, pretty: bool = False, end: str = "\n", indent_guides: bool = True
):
    get_console().print(
        Pretty(val, indent_guides=indent_guides, expand_all=True) if pretty else val,
        style="italic grey50",
        end=end,
    )


def print_info(
    val, *, pretty: bool = False, end: str = "\n", indent_guides: bool = True
):
    get_console().print(
        Pretty(val, indent_guides=indent_guides, expand_all=True) if pretty else val,
        style="cyan",
        end=end,
    )


def print_warning(
    val, *, pretty: bool = False, end: str = "\n", indent_guides: bool = True
):
    get_console().print(
        "[bold blink]WARNING:[/bold blink]",
        Pretty(val, indent_guides=indent_guides, expand_all=True) if pretty else val,
        style="orange1",
        end=end,
    )


def print_error(
    val, *, pretty: bool = False, end: str = "\n", indent_guides: bool = True
):
    get_console().print(
        "[bold blink]ERROR:[/bold blink]",
        Pretty(val, indent_guides=indent_guides, expand_all=True) if pretty else val,
        style="dark_red on white",
        end=end,
    )


def show_spinning_status(text: str):
    """Returns a context manager."""
    return get_console().status(text)


def get_model_title(model_cls: t.Type[BaseModel]) -> str:
    if model_cls.__config__.title:
        return model_cls.__config__.title
    return model_cls.__name__


def get_model_classpath(model_cls: t.Type[BaseModel]) -> str:
    if hasattr(model_cls, "_classpath") and model_cls._classpath:  # type: ignore
        return model_cls._classpath  # type: ignore
    return f"{model_cls.__module__}.{model_cls.__name__}"


def print_model_field_values(
    model_fields: t.Mapping,
    port_values: t.Mapping[str, t.Any],
    icon: str = "",
):
    for k, v in port_values.items():
        rprint(f"\n{icon if icon else '***'} {k}:")
        # Ports might be virtual, as in ShellCommand, so they might not be in the model
        if k in model_fields and model_fields[k].field_info.description:
            rprint(
                f"[italic grey50]{escape(model_fields[k].field_info.description)}[/]"
            )
        rprint(
            "[green]"
            + escape(str(v) if isinstance(v, (bytes, str)) else repr(v))
            + "[/]"
        )


def print_command_inputs(command: "PipelimeCommand"):  # type: ignore # noqa: E602,F821
    print_model_field_values(command.__fields__, command.get_inputs(), _input_icon())


def print_command_outputs(command: "PipelimeCommand"):  # type: ignore # noqa: E602,F821
    print_model_field_values(command.__fields__, command.get_outputs(), _output_icon())


def print_actions_short_help(*actions_info: "ActionInfo", show_class_path: bool = True):
    grid = Table.grid(
        *([Column(overflow="fold") for _ in range(2 + int(show_class_path))]),
        padding=(0, 1),
    )
    for a in actions_info:
        col_vals = [escape(a.name)]
        if show_class_path:
            col_vals.append(f"[italic grey50]{escape(a.classpath)}[/]")
        col_vals.append(escape(a.description))
        grid.add_row(*col_vals)
    rprint(grid)


def print_models_short_help(
    *model_cls: t.Type[BaseModel], show_class_path: bool = True
):
    grid = Table.grid(
        *([Column(overflow="fold") for _ in range(2 + int(show_class_path))]),
        padding=(0, 1),
    )
    for m in model_cls:
        col_vals = [escape(get_model_title(m))]
        if show_class_path:
            col_vals.append(f"[italic grey50]{escape(get_model_classpath(m))}[/]")
        col_vals.append(escape(inspect.getdoc(m) or ""))
        grid.add_row(*col_vals)
    rprint(grid)


def print_model_info(
    model_cls: t.Type[BaseModel],
    *,
    indent_offs: int = 2,
    show_class_path: bool = True,
    show_piper_port: bool = True,
    show_description: bool = True,
    recursive: bool = True,
):
    model_docs = (inspect.getdoc(model_cls) or "") if show_description else ""

    # * Fields, [Description], Type, [Piper Port], Default
    cols = [Column("Fields", overflow="fold")]
    if show_description:
        cols.append(Column("Description", overflow="fold"))
    cols.append(Column("Type", overflow="fold"))
    if show_piper_port:
        cols.append(Column("Piper Port", overflow="fold"))
    cols.append(Column("Default", overflow="fold"))

    grid = Table(
        *cols,
        box=box.SIMPLE_HEAVY,
        title=(
            f"[blue]{_get_signature(model_cls)}[/]"
            + (f"\n\n[italic grey23]{escape(model_docs)}[/]" if model_docs else "")
        ),
        # caption=escape(get_model_classpath(model_cls)) if show_class_path else None,
        title_style="on white",
        title_justify="left",
        expand=True,
    )

    _iterate_model_fields(
        model_cls=model_cls,
        grid=grid,
        indent=0,
        indent_offs=indent_offs,
        show_piper_port=show_piper_port,
        show_description=show_description,
        recursive=recursive,
        add_blank_row=True,
    )

    grid = Panel(
        grid,
        # box=box.HORIZONTALS,
        title=f"[dark_red bold]{escape(get_model_title(model_cls))}[/]",
        subtitle=(
            f"[grey23]{escape(get_model_classpath(model_cls))}[/]"
            if show_class_path
            else None
        ),
    )

    rprint(grid)


def _field_row(
    grid: Table,
    field,
    indent: int,
    indent_offs: int,
    show_piper_port: bool,
    show_description: bool,
    recursive: bool,
):
    is_model = _is_model(field.outer_type_) and not inspect.isabstract(
        field.outer_type_
    )

    if show_description:
        # NB: docs should not come from the inner __root__ type
        if field.field_info.description:
            field_docs = field.field_info.description
        elif hasattr(field.outer_type_, "__doc__") and field.outer_type_.__doc__:
            field_docs = str(inspect.getdoc(field.outer_type_))
        else:
            field_docs = ""

        field_docs = " ".join(field_docs.split())

    has_root_item = ("__root__" in field.outer_type_.__fields__) if is_model else False
    field_outer_type = (
        field.outer_type_.__fields__["__root__"].outer_type_
        if has_root_item
        else field.outer_type_
    )

    if show_piper_port:
        from pipelime.piper import PiperPortType

        fport = str(
            field.field_info.extra.get("piper_port", PiperPortType.PARAMETER).value
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
        + ("[bold salmon1]" if indent == 0 else "")
        + (
            f"{escape(field.name)} / "
            if field.model_config.allow_population_by_field_name and field.has_alias
            else ""
        )
        + f"{escape(field.alias)}"
        + ("[/]" if indent == 0 else "")
    ]

    # Description
    if show_description:
        line.append("▶ " + escape(field_docs))  # type: ignore

    # Type
    line.append(
        ""
        if is_model and not has_root_item and recursive
        else _human_readable_type(field_outer_type).replace("[", r"\[")  # noqa: W605
    )

    # Piper port
    if show_piper_port:
        line.append(fport)  # type: ignore

    # Default value
    line.append("[red]✗[/]" if field.required else f"[green]{field.get_default()}[/]")

    grid.add_row(*line)

    if recursive:
        if is_model and not has_root_item:
            _iterate_model_fields(
                model_cls=field_outer_type,
                grid=grid,
                indent=indent + indent_offs,
                indent_offs=indent_offs,
                show_piper_port=show_piper_port,
                show_description=show_description,
                recursive=recursive,
                add_blank_row=False,
            )
        else:
            inner_types = _get_inner_args(field_outer_type)
            last_types = inner_types
            while last_types:
                last_types = _get_inner_args(*last_types)
                inner_types |= last_types
            inner_types = {
                arg for arg in inner_types if _is_model(arg) and arg is not BaseModel
            }

            for arg in inner_types:
                grid.add_row(
                    (" " * indent)
                    + f"[grey50]{_short_line()} {arg.__name__}[/]"  # type:ignore
                )
                _iterate_model_fields(
                    model_cls=arg,
                    grid=grid,
                    indent=indent + indent_offs,
                    indent_offs=indent_offs,
                    show_piper_port=show_piper_port,
                    show_description=show_description,
                    recursive=recursive,
                    add_blank_row=False,
                )


def _get_signature(model_cls: t.Type[BaseModel]) -> str:
    fullname = {mfield.alias: mfield.name for mfield in model_cls.__fields__.values()}
    excluded = [
        mfield.alias
        for mfield in model_cls.__fields__.values()
        if mfield.field_info.exclude
    ]
    sig = inspect.signature(model_cls)
    sig = sig.replace(
        parameters=[
            p.replace(name=fullname[p.name])
            for p in sig.parameters.values()
            if p.name not in excluded
        ],
        return_annotation=inspect.Signature.empty,
    )
    sig = escape(str(sig))
    for mfield in model_cls.__fields__.values():
        sig = sig.replace(mfield.name, f"\n  [bold salmon1]{mfield.name}[/]")
    return "(\n  " + sig[1:-1] + "\n)"


def _is_model(type_):
    return inspect.isclass(type_) and issubclass(type_, BaseModel)


def _human_readable_type(field_outer_type):
    from enum import Enum

    tstr = display_as_type(field_outer_type)
    if inspect.isclass(field_outer_type) and issubclass(field_outer_type, Enum):
        tstr += " {" + ", ".join(v.name.lower() for v in field_outer_type) + "}"
    return tstr


def _recursive_args_flattening(arg):
    if isinstance(arg, t.Mapping):
        return {
            a
            for k, v in arg.items()
            for u in (_recursive_args_flattening(k), _recursive_args_flattening(v))
            for a in u
        }
    if isinstance(arg, t.Collection) and not isinstance(arg, (str, bytes)):
        return {a for v in arg for a in _recursive_args_flattening(v)}
    return {arg}


def _get_inner_args(*type_):
    return {
        a
        for t_ in type_
        for arg in t.get_args(t_)
        for a in _recursive_args_flattening(arg)
    }


def _iterate_model_fields(
    model_cls,
    grid,
    indent,
    indent_offs,
    show_piper_port,
    show_description,
    recursive,
    add_blank_row,
):
    no_data = True
    for field in model_cls.__fields__.values():  # type: ignore
        if not field.field_info.exclude:
            no_data = False
            _field_row(
                grid,
                field,
                indent=indent,
                indent_offs=indent_offs,
                show_piper_port=show_piper_port,
                show_description=show_description,
                recursive=recursive,
            )
            if add_blank_row:
                grid.add_row()
    if no_data:
        grid.add_row((" " * indent) + "[grey50 italic]([strike]no parameters[/])[/]")
