from typing import List, Mapping, Tuple, Type, Union, cast

from pydantic import BaseModel

from pipelime.cli.utils import PipelimeSymbolsHelper
from pipelime.piper import PipelimeCommand
from pipelime.stages import SampleStage, StageInput


class TuiField(BaseModel):
    title: str
    description: str
    value: Union[str, List["TuiField"]]
    type_: str
    simple: bool


def is_tui_needed(cmd_cls: Type[PipelimeCommand], cmd_args: Mapping) -> bool:
    """Check if the TUI is needed.

    Check if cmd_args contains all the required fields, if not
    the TUI is needed.

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

        if (f not in cmd_args) and (alias.lower() not in cmd_args):
            # if required and not present, return True
            return True

        # if present, check if it's a StageInput
        is_stage_input = is_stageinput(field_info)

        if is_stage_input:
            stage_data = cmd_args.get(f, cmd_args[alias])
            if isinstance(stage_data, Mapping):
                stage_name = list(stage_data.keys())[0]
                stage_args = cast(Mapping, stage_data[stage_name])
            else:
                stage_name = stage_data
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
    schema = stage_cls.schema(by_alias=False)
    required = schema.get("required", [])
    fields = schema["properties"]
    required_fields = [f for f in fields if f in required]

    for f in required_fields:
        alias = fields[f].get("title", "").lower()
        if (f not in stage_args) and (alias.lower() not in stage_args):
            return True

    return False


def is_stageinput(field_info: Mapping) -> bool:
    """Check if the field is a StageInput.

    Args:
        field_info: The field info.

    Returns:
        True if the field is a StageInput, False otherwise.
    """
    field_types = [field.get("$ref", "") for field in field_info.get("allOf", [])]
    return any([StageInput.__name__ in ft for ft in field_types])


def init_field(field_name: str, field_info: Mapping, cmd_args: Mapping) -> TuiField:
    alias = field_info.get("title", "").lower()
    if field_name in cmd_args:
        value = str(cmd_args[field_name])
    elif alias.lower() in cmd_args:
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
    alias = field_info.get("title", "").lower()

    if (field_name not in cmd_args) and (alias.lower() not in cmd_args):
        description = field_info.get("description", "")
        field = TuiField(
            title=field_name,
            description=description,
            value="",
            type_="str",
            simple=True,
        )
        return field

    stage_data = cmd_args.get(field_name, cmd_args[alias])
    if isinstance(stage_data, Mapping):
        stage_name = list(stage_data.keys())[0]
        stage_args = cast(Mapping, stage_data[stage_name])
    else:
        stage_name = stage_data
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
        value=fields,
        type_="",
        simple=False,
    )
    return field
