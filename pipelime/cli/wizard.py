from typing import Optional, Union, Type
from pydantic import BaseModel
import pipelime.cli.pretty_print as ppp


def model_cfg_wizard(model_cls: Union[Type[BaseModel], str]):
    import yaml

    ppp.print_info("\n\U0001FA84  Configuration Wizard")
    ppp.print_info("- `\"` or `'` to enforce string values")
    ppp.print_info(
        "- `[` and `{` followed by a new-line to start a list or dict "
        "(one item per line, use `:` to separate key and value)"
    )
    ppp.print_info("- `]` and `}` followed by a new-line to end a list or dict")
    ppp.print_info("- `? <symbol>` to begin a wizard configuration for a symbol")

    model_cls = _get_model_cls(model_cls, True)  # type: ignore
    if model_cls is None:
        return

    cfg = _iterate_model_fields(model_cls)

    ppp.print_info("\n\u2728 CONFIG YAML")
    ppp.print_info("==============")
    ppp.print_info(yaml.safe_dump(cfg))


def _get_model_cls(
    name_or_cls: Union[Type[BaseModel], str], should_be_cmd: bool
) -> Optional[Type[BaseModel]]:
    if isinstance(name_or_cls, str):
        from pipelime.cli.utils import PipelimeSymbolsHelper

        if should_be_cmd:
            cmd_cls = PipelimeSymbolsHelper.get_command(name_or_cls)
        else:
            cmd_cls = PipelimeSymbolsHelper.get_operator(name_or_cls)
            if cmd_cls is None:
                cmd_cls = PipelimeSymbolsHelper.get_stage(name_or_cls)
        if cmd_cls is None:
            PipelimeSymbolsHelper.show_error_and_help(
                name_or_cls,
                should_be_cmd=should_be_cmd,
                should_be_op=not should_be_cmd,
                should_be_stage=not should_be_cmd,
            )
            return None
        name_or_cls = cmd_cls[1]
    return name_or_cls  # type: ignore


def _get_value_list(prefix):
    from rich.prompt import Prompt

    value_list = []
    while True:
        value = Prompt.ask(f"{prefix} []")
        if value == "]":
            return value_list
        value_list.append(_decode_value(value, prefix + f"[{len(value)}]"))


def _get_value_map(prefix):
    from rich.prompt import Prompt

    value_map = {}
    while True:
        key_val = Prompt.ask(f"{prefix} " "{}")
        if key_val == "}":
            return value_map
        key, _, value = key_val.partition(":")
        value_map[key] = _decode_value(value, prefix + "." + key)


def _get_general_field_value(prefix, default=...):
    from rich.prompt import Prompt

    value = Prompt.ask("Enter value", default=default)
    return _decode_value(value, prefix)


def _decode_value(value, prefix):
    value = value.strip()
    if len(value) > 1 and value[0] in ("'", '"') and value[-1] == value[0]:
        return value[1:-1]
    if value.lower() in (None, "", "none", "null", "nul"):
        return None
    if value == "[":
        return _get_value_list(prefix)
    if value == "{":
        return _get_value_map(prefix)
    if value[0] == "?":
        model_cls = _get_model_cls(value[1:].strip(), False)
        if model_cls is None:
            ppp.print_error("Invalid symbol")
            return None
        return _iterate_model_fields(model_cls, prefix + ".")
    return value


def _get_field_value(field, prefix=""):
    from rich.prompt import Prompt, Confirm

    is_model = ppp._is_model(field.outer_type_)
    has_root_item = ("__root__" in field.outer_type_.__fields__) if is_model else False
    field_outer_type = (
        field.outer_type_.__fields__["__root__"].outer_type_
        if has_root_item
        else field.outer_type_
    )

    ppp.print_info(f"\n{prefix}{field.name} ({field_outer_type})")

    if field.required:
        if is_model:
            return _iterate_model_fields(field_outer_type, prefix + field.name + ".")
        return _get_general_field_value(prefix=prefix + field.name)
    else:
        default_value = field.get_default()
        if is_model:
            if default_value is not None:
                default_value = default_value.dict()
                ppp.print_debug("----Default:")
                ppp.print_debug(default_value)
            else:
                ppp.print_debug("----Default: None")

            if Confirm.ask("Accept default?", default=True):
                return default_value
            return _iterate_model_fields(field_outer_type, prefix + field.name + ".")

        return _get_general_field_value(
            prefix=prefix + field.name, default=str(default_value)
        )


def _iterate_model_fields(model_cls, prefix=""):
    cfg = {}
    for field in model_cls.__fields__.values():
        if not field.field_info.exclude:
            cfg[field.name] = _get_field_value(field, prefix)
    return cfg
