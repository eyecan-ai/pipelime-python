import json
from ast import literal_eval
from enum import Enum
from json import JSONDecodeError
from typing import Any, List, Mapping, Optional, Tuple, Type, cast

import yaml
from pydantic import BaseModel
from pydantic.fields import ModelField
from yaml.error import YAMLError

from pipelime.cli.utils import PipelimeSymbolsHelper
from pipelime.piper import PipelimeCommand
from pipelime.stages import SampleStage, StageInput


class TuiField(BaseModel):
    name: str
    description: Optional[str]
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
    for field in cmd_cls.__fields__.values():
        name = field.name
        alias = field.alias
        required = field.required

        if (name not in cmd_args) and (alias not in cmd_args) and required:
            # if required and not present, return True
            return True

        # if present, check if it's a StageInput
        if field.type_ == StageInput:
            stage_input_args = cmd_args.get(name, cmd_args[alias])

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
    for field in stage_cls.__fields__.values():
        name = field.name
        alias = field.alias
        required = field.required

        if (name not in stage_args) and (alias not in stage_args) and required:
            # if required and not present, return True
            return True

    return False


def init_tui_field(field: ModelField, cmd_args: Mapping) -> TuiField:
    """Initialize a TuiField.

    Args:
        field: The field from the parent pydantic model.
        cmd_args: The args provided by the user (if any).

    Returns:
        The initialized TuiField.
    """
    if field.name in cmd_args:
        value = str(cmd_args[field.name])
    elif field.alias in cmd_args:
        value = str(cmd_args[field.alias])
    else:
        field_default = field.get_default()
        if isinstance(field_default, Enum):
            field_default = field_default.value
        value = str(field_default)

    tui_field = TuiField(
        name=field.name,
        description=field.field_info.description,
        value=value,
        type_=str(field.type_),
        simple=True,
    )
    return tui_field


def init_stageinput_tui_field(field: ModelField, cmd_args: Mapping) -> TuiField:
    """Initialize a TuiField for a StageInput.

    Args:
        field: The field from the parent pydantic model.
        cmd_args: The args provided by the user (if any).

    Returns:
        The initialized TuiField.
    """
    if (field.name not in cmd_args) and (field.alias not in cmd_args):
        tui_field = TuiField(
            name=field.name,
            description=field.field_info.description,
            value="",
            type_=str(field.type_),
            simple=True,
        )
        return tui_field

    stage_input_args = cmd_args.get(field.name, cmd_args[field.alias])
    if isinstance(stage_input_args, Mapping):
        stage_name = list(stage_input_args.keys())[0]
        stage_args = cast(Mapping, stage_input_args[stage_name])
    else:
        stage_name = stage_input_args
        stage_args = {}

    stage_info = PipelimeSymbolsHelper.get_stage(stage_name)
    stage_info = cast(Tuple[str, str, Type[SampleStage]], stage_info)
    stage_cls = stage_info[-1]

    tui_fields = []
    for field in stage_cls.__fields__.values():
        tui_fields.append(init_tui_field(field, stage_args))

    tui_field = TuiField(
        name=stage_name,
        description="",
        values=tui_fields,
        type_="",
        simple=False,
    )
    return tui_field


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
