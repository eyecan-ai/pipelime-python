from typing import Optional, Union, Type, Any, Optional
from copy import deepcopy
from pydantic import BaseModel
from pydantic.typing import display_as_type
import pipelime.cli.pretty_print as ppp


class InvalidValue(Exception):
    pass


class ColoredPath:
    COLORS = [
        "[#FF7F00]",
        "[#33A02C]",
        "[#6A3D9A]",
        "[#E31A1C]",
        "[#B15928]",
        "[#1F78B4]",
        "[#FDBF6F]",
        "[#B2DF8A]",
        "[#CAB2D6]",
        "[#FB9A99]",
        "[#FFFF99]",
        "[#A6CEE3]",
    ]

    def __init__(self, path: str = "", cidx: int = -1):
        self.path = path
        self.cidx = cidx

    def as_map(self, path: str):
        cidx = (self.cidx + 1) % len(self.COLORS)
        col_hex = self.COLORS[cidx]
        return ColoredPath(
            ".".join(filter(None, [self.path, col_hex + path + "[/]"])), cidx=cidx
        )

    def as_list(self, path: str):
        cidx = (self.cidx + 1) % len(self.COLORS)
        col_hex = self.COLORS[cidx]
        return ColoredPath(
            self.path + col_hex + "\[" + path + "][/]", cidx=cidx  # type: ignore
        )

    def __str__(self) -> str:
        return str(self.path)

    def __repr__(self) -> str:
        return repr(self.path)


class FieldModel:
    def __init__(
        self,
        name: str,
        outer_type: Type,
        description: Optional[str],
        is_required: bool,
        default_value: Any,
        is_model: bool,
    ):
        self.name = name
        self.outer_type = outer_type
        self.description = description
        self.is_required = is_required
        self.default_value = default_value
        self.is_model = is_model


def model_cfg_wizard(model_cls: Union[Type[BaseModel], str]):
    import yaml

    ppp.print_info("\n🪄  Configuration Wizard")
    ppp.print_info("- `\"` or `'` to enforce string values")
    ppp.print_info(
        "- `[` and `{` followed by a new-line to start a list or dict "
        "(one item per line, use `:` to separate key and value)"
    )
    ppp.print_info("- `]` and `}` followed by a new-line to end a list or dict")
    ppp.print_info(
        "- `< \[model]` to begin a wizard configuration for a pydantic model "  # type: ignore
        "(should be explicitly listed in the type list)."
    )
    ppp.print_info(
        "- `? \[class.path]` to begin a wizard configuration "  # type: ignore
        "for a choixe `$call` directive."
    )
    ppp.print_info("- `! \[class.path]` to add a choixe `$symbol` directive.")  # type: ignore
    ppp.print_info(
        "- `# \[name]` to begin a wizard configuration "  # type: ignore
        "for a pipelime command, stage or operation."
    )
    ppp.print_info(
        "- `c# \[name]`, `s# \[name]`, `o# \[name]` as above, "  # type: ignore
        "but specifying the type."
    )

    try:
        if isinstance(model_cls, str):
            model_cls = _get_pipelime_type(model_cls, True)  # type: ignore
    except InvalidValue:
        return

    cfg = _iterate_model_fields(model_cls)  # type: ignore

    ppp.print_info("\n✨ CONFIG YAML")
    ppp.print_info("==============")
    ppp.print_info(yaml.safe_dump(cfg, sort_keys=False))


def _get_pipelime_type(
    name: str, is_command: bool = True, is_stage: bool = True, is_operation: bool = True
) -> Type[BaseModel]:
    from pipelime.cli.utils import PipelimeSymbolsHelper

    cmd_cls = None
    if is_command:
        cmd_cls = PipelimeSymbolsHelper.get_command(name)
    if cmd_cls is None and is_stage:
        cmd_cls = PipelimeSymbolsHelper.get_stage(name)
    if cmd_cls is None and is_operation:
        cmd_cls = PipelimeSymbolsHelper.get_operator(name)

    if cmd_cls is None:
        PipelimeSymbolsHelper.show_error_and_help(
            name,
            should_be_cmd=is_command,
            should_be_op=is_operation,
            should_be_stage=is_stage,
        )
        raise InvalidValue()

    return cmd_cls[1]


def _get_value_list(prefix: ColoredPath):
    from rich.prompt import Prompt

    value_list = []
    while True:
        value = Prompt.ask(f"{prefix}[{len(value_list)}]")
        if value == "]":
            return value_list
        try:
            value_list.append(
                _decode_value(value, prefix.as_list(str(len(value_list))))
            )
        except InvalidValue:
            pass


def _get_value_map(prefix: ColoredPath):
    from rich.prompt import Prompt

    value_map = {}
    while True:
        key_val = Prompt.ask(f"{prefix}" "{}")
        if key_val == "}":
            return value_map
        key, _, value = key_val.partition(":")
        try:
            value_map[key] = _decode_value(value, prefix.as_map(key))
        except InvalidValue:
            pass


def _get_general_field_value(prefix: ColoredPath, default=...):
    from rich.prompt import Prompt

    while True:
        value = Prompt.ask("Enter value", default=default)
        try:
            return _decode_value(value, prefix)
        except InvalidValue:
            pass


def _decode_value(value, prefix: ColoredPath):
    from pipelime.choixe.utils.imports import import_symbol

    value = value.strip()
    if len(value) > 1 and value[0] in ("'", '"') and value[-1] == value[0]:
        return value[1:-1]
    if value.lower() in (None, "", "none", "null", "nul"):
        return None
    if value == "[":
        return _get_value_list(prefix)
    if value == "{":
        return _get_value_map(prefix)
    if value[0] == "<":
        v = value[1:].strip()
        if not v:
            raise InvalidValue()
        try:
            model_cls = import_symbol(v)
        except ImportError:
            ppp.print_error(f"Cannot import model class {v}")
            raise InvalidValue()
        try:
            return _iterate_model_fields(model_cls, prefix)
        except TypeError as e:
            ppp.print_error(f"Invalid symbol: {v} ({e})")
            raise InvalidValue()
    if value[0] == "?":
        v = value[1:].strip()
        if not v:
            raise InvalidValue()
        try:
            clb_type = import_symbol(v)
        except ImportError:
            ppp.print_error(f"Cannot import callable {v}")
            raise InvalidValue()
        try:
            args = (
                _iterate_model_fields(clb_type, prefix)
                if ppp._is_model(clb_type)
                else _iterate_callable_args(clb_type, prefix)
            )
            return {"$call": v, "$args": args}
        except TypeError as e:
            ppp.print_error(f"Invalid symbol: {v} ({e})")
            raise InvalidValue()
    if value[0] == "!":
        v = value[1:].strip()
        if not v:
            raise InvalidValue()
        try:
            sym_type = import_symbol(v)
        except ImportError:
            ppp.print_warning(
                f"{v} {ppp._short_line()} Symbol will be included, but importing has failed"
            )
        return f"$symbol({v})"
    if value[0] == "#" or (len(value) > 1 and value[1] == "#"):
        v = value[1:].strip() if value[0] == "#" else value[2:].strip()
        if not v:
            raise InvalidValue()

        pl_type = _get_pipelime_type(
            v,
            is_command=value[0] in ("#", "c"),
            is_stage=value[0] in ("#", "s"),
            is_operation=value[0] in ("#", "o"),
        )
        args = _iterate_model_fields(pl_type, prefix)
        return {v: args}

    return value


def _get_field_value(field: FieldModel, prefix: ColoredPath):
    from rich.table import Table
    from rich import box
    from rich.prompt import Confirm
    from rich.pretty import Pretty
    from rich import print as rprint

    field_prefix = prefix.as_map(field.name)
    header = [
        f"{field_prefix}\n«"
        + display_as_type(field.outer_type).replace("[", r"\[")
        + "»"
    ]
    if not field.is_required:
        header.append("Default")

    desc = field.description if field.description else ""

    table = Table(
        *header,
        box=box.SIMPLE_HEAVY,
        show_header=True,
        show_footer=False,
        show_lines=True,
        width=80,
    )

    if field.is_required:
        table.add_row(desc)
        rprint(table)
        if field.is_model:
            return _get_general_field_value(
                prefix=field_prefix,
                default=f"< {field.outer_type.__module__}.{field.outer_type.__name__}",
            )
        return _get_general_field_value(prefix=field_prefix)
    else:
        default_value = field.default_value
        if field.is_model:
            if default_value is not None:
                default_value = default_value.dict()
                table.add_row(
                    desc, Pretty(default_value, indent_guides=True, expand_all=True)
                )
            else:
                table.add_row(desc, "None")

            rprint(table)
            if Confirm.ask("Accept default?", default=True):
                return default_value
            return _get_general_field_value(
                prefix=field_prefix,
                default=f"< {field.outer_type.__module__}.{field.outer_type.__name__}",
            )

        table.add_row(desc, Pretty(default_value, indent_guides=True, expand_all=True))
        rprint(table)
        return _get_general_field_value(prefix=field_prefix, default=str(default_value))


def _iterate_model_fields(model_cls: Type[BaseModel], prefix=ColoredPath()):
    if not ppp._is_model(model_cls):
        raise TypeError("not a pydantic model")

    cfg = {}
    for field in model_cls.__fields__.values():
        if not field.field_info.exclude:
            is_model = ppp._is_model(field.outer_type_)
            has_root_item = (
                ("__root__" in field.outer_type_.__fields__) if is_model else False
            )
            field_outer_type = (
                field.outer_type_.__fields__["__root__"].outer_type_
                if has_root_item
                else field.outer_type_
            )

            try:
                cfg[field.name] = _get_field_value(
                    FieldModel(
                        name=field.name,
                        outer_type=field_outer_type,
                        description=field.field_info.description,
                        is_required=bool(field.required),
                        default_value=field.get_default(),
                        is_model=is_model,
                    ),
                    prefix,
                )
            except InvalidValue:
                pass
    return cfg


def _iterate_callable_args(clb_type, prefix=ColoredPath()):
    import inspect

    cfg = {}
    for field in inspect.signature(
        clb_type.__init__ if inspect.isclass(clb_type) else clb_type
    ).parameters.values():
        if field.name in ("self", "cls"):
            continue
        if field.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.POSITIONAL_ONLY,
        ):
            raise TypeError(
                f"{inspect.Parameter.VAR_POSITIONAL.description} and "
                f"{inspect.Parameter.POSITIONAL_ONLY.description} parameters "
                "are not supported"
            )

        try:
            cfg[field.name] = _get_field_value(
                FieldModel(
                    name=field.name,
                    outer_type=Any
                    if field.annotation is inspect.Parameter.empty
                    else field.annotation,
                    description=None,
                    is_required=field.default is inspect.Parameter.empty,
                    default_value=field.default,
                    is_model=False,
                ),
                prefix,
            )
        except InvalidValue:
            pass
    return cfg
