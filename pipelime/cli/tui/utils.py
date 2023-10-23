from enum import Enum
from typing import List, Mapping, Tuple, Type, cast

from pydantic import BaseModel
from pydantic.fields import ModelField

from pipelime.cli.utils import PipelimeSymbolsHelper
from pipelime.piper import PipelimeCommand
from pipelime.stages import SampleStage, StageInput


class TuiField(BaseModel):
    simple: bool
    name: str
    description: str = ""
    hint: str = ""
    type_: str = ""
    values: List["TuiField"] = []
    value: str = ""


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
            if name in cmd_args:
                stage_input_args = cmd_args.get(name)
            else:
                stage_input_args = cmd_args.get(alias)

            stage_name = ""
            stage_args = {}
            if isinstance(stage_input_args, Mapping):
                stage_name = list(stage_input_args.keys())[0]
                stage_args = cast(Mapping, stage_input_args[stage_name])
            elif isinstance(stage_input_args, str):
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
            return False

    return True


def init_tui_field(field: ModelField, args: Mapping) -> TuiField:
    """Initialize a TuiField.

    Args:
        field: The field from the parent pydantic model.
        args: The args provided by the user (if any).

    Returns:
        The initialized TuiField.
    """
    default = ""
    hint = ""

    if field.name in args:
        default = str(args[field.name])
    elif field.alias in args:
        default = str(args[field.alias])
    else:
        field_default = field.get_default()

        if field_default is not None:
            if isinstance(field_default, BaseModel):
                hint = str(field_default)
            else:
                if isinstance(field_default, Enum):
                    field_default = field_default.value
                default = str(field_default)

    tui_field = TuiField(
        simple=True,
        name=field.name,
        description=str(field.field_info.description),
        hint=hint,
        type_=get_field_type(field),
        value=default,
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
            simple=True,
            name=field.name,
            description=str(field.field_info.description),
            type_=get_field_type(field),
        )
        return tui_field

    if field.name in cmd_args:
        stage_input_args = cmd_args.get(field.name)
    else:
        stage_input_args = cmd_args.get(field.alias)

    stage_name = ""
    stage_args = {}
    if isinstance(stage_input_args, Mapping):
        stage_name = list(stage_input_args.keys())[0]
        stage_args = cast(Mapping, stage_input_args[stage_name])
    elif isinstance(stage_input_args, str):
        stage_name = stage_input_args
        stage_args = {}

    stage_info = PipelimeSymbolsHelper.get_stage(stage_name)
    stage_info = cast(Tuple[str, str, Type[SampleStage]], stage_info)
    stage_cls = stage_info[-1]

    tui_fields = []
    for field in stage_cls.__fields__.values():
        tui_fields.append(init_tui_field(field, stage_args))

    tui_field = TuiField(
        simple=False,
        name=stage_name,
        description=str(stage_cls.__doc__),
        values=tui_fields,
    )
    return tui_field


def get_field_type(field: ModelField) -> str:
    """Get the type of a field.

    Args:
        field: The field from the parent pydantic model.

    Returns:
        The type of the field.
    """
    type_ = field.annotation

    if "typing." in str(type_):
        type_ = str(type_).replace("typing.", "")
    else:
        type_ = field.annotation.__name__

    # replace common pipelime types
    common_pipelime_types = {
        "pipelime.piper.model.PipelimeCommand": "PipelimeCommand",
        "pipelime.sequences.sample.Sample": "Sample",
        "pipelime.stages.base.SampleStage": "SampleStage",
        "pipelime.stages.base.StageInput": "StageInput",
        "pipelime.commands.interfaces.InputDatasetInterface": "InputDatasetInterface",
        "pipelime.commands.interfaces.OutputDatasetInterface": "OutputDatasetInterface",
        "pipelime.commands.interfaces.GrabberInterface": "GrabberInterface",
    }
    for k, v in common_pipelime_types.items():
        type_ = type_.replace(k, v)

    return type_
