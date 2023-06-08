import json
from ast import literal_eval
from json import JSONDecodeError
from typing import Any, List, Mapping, Tuple, Type, cast

import yaml
from pydantic import BaseModel
from yaml.error import YAMLError

from pipelime.cli.utils import PipelimeSymbolsHelper
from pipelime.piper import PipelimeCommand
from pipelime.stages import SampleStage, StageInput


class TuiField(BaseModel):
    title: str
    description: str
    value: str = ""
    values: List["TuiField"] = []
    type_: str
    simple: bool


def is_tui_needed(cmd_cls: Type[PipelimeCommand], cmd_args: Mapping) -> bool:
    """Check if the TUI is needed.

    Check if cmd_args contains all the required fields, if not the TUI is needed.

    Args:
        cmd_cls: The pipelime command class.
        cmd_args: The args provided by the user (if any).

    Returns:
        True if the TUI is needed, False otherwise.
    """
    schema = cmd_cls.schema(by_alias=False)

    required = schema.get("required", [])
    fields = schema["properties"]
    required_fields = [f for f in fields if f in required]

    for f in required_fields:
        field_info = fields[f]
        alias = field_info.get("title", "").lower()

        if (f not in cmd_args) and (alias not in cmd_args):
            # if required and not present, return True
            return True

        # if present, check if it's a StageInput
        if is_stageinput(field_info):
            stage_input_args = cmd_args.get(f, cmd_args[alias])

            if isinstance(stage_input_args, Mapping):
                stage_name = list(stage_input_args.keys())[0]
                stage_args = cast(Mapping, stage_input_args[stage_name])
            else:
                stage_name = stage_input_args
                stage_args = {}

            stage_info = PipelimeSymbolsHelper.get_stage(stage_name)
            if stage_info is None:
                # stage not found, return False to let pipelime handle the error
                return False

            stage_cls = stage_info[-1]
            if not are_stageinput_args_present(stage_cls, stage_args):
                return True

    return False


def are_stageinput_args_present(
    stage_cls: Type[SampleStage],
    stage_args: Mapping,
) -> bool:
    """Check if a StageInput required args are present.

    Args:
        stage_cls: The pipelime stage class.
        stage_args: The args provided by the user for the stage (if any).

    Returns:
        True if the StageInput required args are present, False otherwise.
    """
    schema = stage_cls.schema(by_alias=False)
    required = schema.get("required", [])
    fields = schema["properties"]
    required_fields = [f for f in fields if f in required]

    for f in required_fields:
        alias = fields[f].get("title", "").lower()
        if (f not in stage_args) and (alias not in stage_args):
            return True

    return False


def is_stageinput(field_info: Mapping) -> bool:
    """Check if the field is a StageInput.

    Args:
        field_info: The field info from the parent schema.

    Returns:
        True if the field is a StageInput, False otherwise.
    """
    field_types = [field.get("$ref", "") for field in field_info.get("allOf", [])]
    return any([StageInput.__name__ in ft for ft in field_types])


def init_field(field_name: str, field_info: Mapping, cmd_args: Mapping) -> TuiField:
    """Initialize a TuiField.

    Args:
        field_name: The field name.
        field_info: The field info from the parent schema.
        cmd_args: The args provided by the user (if any).

    Returns:
        The initialized TuiField.
    """
    alias = field_info.get("title", "").lower()
    if field_name in cmd_args:
        value = str(cmd_args[field_name])
    elif alias in cmd_args:
        value = str(cmd_args[alias])
    else:
        default = field_info.get("default", "")
        value = str(default)

    description = field_info.get("description", "")

    field = TuiField(
        title=field_name,
        description=description,
        value=value,
        type_="str",
        simple=True,
    )
    return field


def init_stageinput_field(
    field_name: str,
    field_info: Mapping,
    cmd_args: Mapping,
) -> TuiField:
    """Initialize a TuiField for a StageInput.

    Args:
        field_name: The field name.
        field_info: The field info from the parent schema.
        cmd_args: The args provided by the user (if any).

    Returns:
        The initialized TuiField.
    """
    alias = field_info.get("title", "").lower()

    if (field_name not in cmd_args) and (alias not in cmd_args):
        description = field_info.get("description", "")
        field = TuiField(
            title=field_name,
            description=description,
            value="",
            type_="str",
            simple=True,
        )
        return field

    stage_input_args = cmd_args.get(field_name, cmd_args[alias])
    if isinstance(stage_input_args, Mapping):
        stage_name = list(stage_input_args.keys())[0]
        stage_args = cast(Mapping, stage_input_args[stage_name])
    else:
        stage_name = stage_input_args
        stage_args = {}

    stage_info = PipelimeSymbolsHelper.get_stage(stage_name)
    stage_info = cast(Tuple[str, str, Type[SampleStage]], stage_info)
    stage_cls = stage_info[-1]
    schema = stage_cls.schema(by_alias=False)
    stage_fields = schema["properties"]

    fields = []
    for f in stage_fields:
        field_info = stage_fields[f]
        fields.append(init_field(f, field_info, stage_args))

    field = TuiField(
        title=stage_name,
        description="",
        values=fields,
        type_="",
        simple=False,
    )
    return field


def parse_value(s: str) -> Any:
    """Parse a string value.

    The function tries to parse the value as YAML, JSON and Python literal,
    returning the first successful parse.

    Args:
        s: The string value to parse.

    Returns:
        The parsed value.
    """
    value = s

    if len(value) > 0:
        parse_fns = [yaml.safe_load, json.loads, literal_eval]
        while parse_fns:
            try:
                value = parse_fns.pop(0)(value)
                break
            except (YAMLError, JSONDecodeError, ValueError, SyntaxError):
                pass

    return value
