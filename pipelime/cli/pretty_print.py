import inspect
import typing as t

from pydantic import BaseModel, RootModel
from pydantic_core import PydanticUndefined
from rich import box, get_console
from rich import print as rprint
from rich.markup import escape
from rich.panel import Panel
from rich.pretty import Pretty
from rich.table import Column, Table

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
    if model_cls.model_config.get("title"):
        return model_cls.model_config.get("title")
    return model_cls.__name__


def get_model_classpath(model_cls: t.Type[BaseModel]) -> str:
    if hasattr(model_cls, "_classpath") and model_cls._classpath:  # type: ignore
        return model_cls._classpath  # type: ignore
    return f"{model_cls.__module__}.{model_cls.__qualname__}"


def print_model_field_values(
    model_fields: t.Mapping,
    port_values: t.Mapping[str, t.Any],
    icon: str = "",
):
    for k, v in port_values.items():
        rprint(f"\n{icon if icon else '***'} {k}:")
        # Ports might be virtual, as in ShellCommand, so they might not be in the model
        if k in model_fields and model_fields[k].description:
            rprint(
                f"[italic grey50]{escape(model_fields[k].description)}[/]"
            )
        rprint(
            "[green]"
            + escape(str(v) if isinstance(v, (bytes, str)) else repr(v))
            + "[/]"
        )


def print_command_inputs(command: "PipelimeCommand"):  # type: ignore # noqa: E602,F821
    print_model_field_values(
        type(command).model_fields, command.get_inputs(), _input_icon()
    )


def print_command_outputs(command: "PipelimeCommand"):  # type: ignore # noqa: E602,F821
    print_model_field_values(
        type(command).model_fields, command.get_outputs(), _output_icon()
    )


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
        col_vals.append(escape((inspect.getdoc(m) or "").partition("\n")[0]))
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
            f"[#5fafff]{_get_signature(model_cls)}[/]"
            + (f"\n\n[italic grey82]{escape(model_docs)}[/]" if model_docs else "")
        ),
        title_style="on #293a05",
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
        title=f"[dark_orange bold][on #293a05]{escape(get_model_title(model_cls))}[/]",
        subtitle=(
            f"[#5fafff][on #293a05]{escape(get_model_classpath(model_cls))}[/]"
            if show_class_path
            else None
        ),
    )

    rprint(grid)


def _outer_type(ann):
    """Strip Optional/Union[..., None] and return the underlying type (v1 outer_type_)."""
    if t.get_origin(ann) is t.Union:
        args = [a for a in t.get_args(ann) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return ann


def _field_row(
    grid: Table,
    name: str,
    field,
    indent: int,
    indent_offs: int,
    show_piper_port: bool,
    show_description: bool,
    recursive: bool,
):
    from enum import Enum

    json_extra = field.json_schema_extra or {}
    expand_help = json_extra.get("expand_help", False)

    field_core = _outer_type(field.annotation)
    is_model = _is_model(field_core) and not inspect.isabstract(field_core)

    if show_description:
        # NB: docs should not come from the inner __root__ type
        if field.description:
            field_docs = field.description
        elif hasattr(field_core, "__doc__") and field_core.__doc__:
            field_docs = str(inspect.getdoc(field_core))
        else:
            field_docs = ""

        # field_docs = " ".join(field_docs.split())

    has_root_item = is_model and issubclass(field_core, RootModel)
    field_outer_type = (
        field_core.model_fields["root"].annotation if has_root_item else field_core
    )

    if show_piper_port:
        from pipelime.piper import PiperPortType

        fport = str(
            json_extra.get("piper_port", PiperPortType.PARAMETER).value
        ).upper()

        if fport == PiperPortType.INPUT.value.upper():
            fport = f"{_input_icon()} [yellow]{fport}[/]"
        elif fport == PiperPortType.OUTPUT.value.upper():
            fport = f"{_output_icon()} [cyan]{fport}[/]"
        else:
            fport = f"{_parameter_icon()} {fport}"

    # Field name & alias
    has_alias = field.alias is not None and field.alias != name
    line = [
        (" " * indent)
        + ("[bold dark_orange]" if indent == 0 else "")
        + (f"{escape(name)} / " if has_alias else "")
        + f"{escape(field.alias or name)}"
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
    field_default = field.get_default(call_default_factory=True)
    if field_default is PydanticUndefined:
        field_default = None
    if isinstance(field_default, Enum):
        field_default = field_default.value
    line.append("[red]✗[/]" if field.is_required() else f"[green]{field_default}[/]")

    grid.add_row(*line)

    if recursive or expand_help:
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
    class RichParameter(inspect.Parameter):
        def __init__(self, *, name, kind, default, annotation):
            self._name = name
            self._kind = kind
            self._default = default
            self._annotation = annotation

        def __str__(self):
            kind = self.kind
            formatted = self._name

            color = "dark_orange"
            if kind == inspect.Parameter.VAR_POSITIONAL:
                formatted = "*" + formatted
            elif kind == inspect.Parameter.VAR_KEYWORD:
                formatted = "**<any-extra-data>"
                color = "indian_red"

            formatted = f"[bold {color}]{escape(formatted)}[/]"

            # Add annotation and default value
            if self._annotation is not inspect._empty:
                formatted = "{}: {}".format(
                    formatted,
                    escape(
                        inspect.formatannotation(self._annotation).replace(
                            "NoneType", "None"
                        )
                    ),
                )

            if self._default is not inspect._empty:
                default_str = escape(repr(self._default))
                if self._annotation is not inspect._empty:
                    formatted = "{} = {}".format(formatted, default_str)
                else:
                    formatted = "{}={}".format(formatted, default_str)

            return f"\n  {formatted}"

    fullname = {
        (mfield.alias or mname): mname
        for mname, mfield in model_cls.model_fields.items()
    }
    excluded = [
        (mfield.alias or mname)
        for mname, mfield in model_cls.model_fields.items()
        if mfield.exclude
    ]

    sig = inspect.signature(model_cls)
    sig = sig.replace(
        parameters=[
            RichParameter(
                name=fullname.get(p.name, p.name),
                kind=p.kind,
                default=p.default,
                annotation=p.annotation,
            )
            for p in sig.parameters.values()
            if p.name not in excluded
        ],
        return_annotation=inspect.Signature.empty,
    )

    sig = (
        str(sig)
        .replace("/,", "\n  /,")
        .replace("*,", "\n  *,")
        .replace("/)", "\n  /)")
        .replace("*)", "\n  *)")
    )
    sig = sig[:-1] + "\n)"
    return sig


def _is_model(type_):
    return inspect.isclass(type_) and issubclass(type_, BaseModel)


def _human_readable_type(field_outer_type):
    import types as _types
    from enum import Enum
    from typing import get_args, get_origin

    def is_union(o):
        if o is t.Union:
            return True
        union_type = getattr(_types, "UnionType", None)
        return union_type is not None and o is union_type

    v = field_outer_type
    if not (isinstance(v, type) or get_origin(v) is not None or v is None):
        v = v.__class__

    v_orig = get_origin(v)

    if is_union(v_orig):
        return " | ".join(map(_human_readable_type, get_args(v)))
    if inspect.isclass(v_orig):
        if issubclass(dict, v_orig):
            return "{" + ": ".join(map(_human_readable_type, get_args(v))) + "}"
        if issubclass(list, v_orig):
            return "[" + ", ".join(map(_human_readable_type, get_args(v))) + ", ...]"
        if issubclass(tuple, v_orig):
            return "(" + ", ".join(map(_human_readable_type, get_args(v))) + ")"

    if inspect.isclass(v) and issubclass(v, Enum):
        v = v.__name__ + "{" + ", ".join(e.name.lower() for e in v) + "}"
    elif get_origin(v) is not None:
        # Generic alias are constructs like `list[int]`
        v = str(v).replace("typing.", "")
    else:
        try:
            v = v.__name__
        except AttributeError:
            # happens with typing objects
            v = str(v).replace("typing.", "")

    return v.replace("NoneType", "None")


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
    for name, field in model_cls.model_fields.items():  # type: ignore
        if not field.exclude:
            no_data = False
            _field_row(
                grid,
                name,
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
