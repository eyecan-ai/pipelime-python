import asyncio
from enum import Enum
from pathlib import Path
from textwrap import fill
from typing import Any, Dict, List, Optional, Tuple, Type, Union, cast

import pytest
from pydantic import BaseModel, Field
from textual.keys import Keys

from pipelime.cli.tui import TuiApp, is_tui_needed
from pipelime.cli.tui.utils import (
    TuiField,
    are_stageinput_args_present,
    get_field_type,
    init_stageinput_tui_field,
    init_tui_field,
    parse_value,
)
from pipelime.cli.utils import PipelimeSymbolsHelper
from pipelime.commands import MapCommand
from pipelime.commands.interfaces import (
    GrabberInterface,
    InputDatasetInterface,
    OutputDatasetInterface,
)
from pipelime.piper import PipelimeCommand, PiperPortType
from pipelime.sequences import Sample
from pipelime.stages import SampleStage, StageInput, StageRemap


class FooCommand(PipelimeCommand, title="foo-command"):
    """This is a test command, with a test description which is very long to test how
    the TUI handles long descriptions.
    """

    input_folder: InputDatasetInterface = InputDatasetInterface.pyd_field(
        alias="i",
        description="Path to the input folder",
        piper_port=PiperPortType.INPUT,
    )
    output_folder: OutputDatasetInterface = OutputDatasetInterface.pyd_field(
        description="Path to the output folder",
        piper_port=PiperPortType.OUTPUT,
    )
    debug: bool = Field(
        False,
        description="Whether to show debug information",
    )
    grabber: GrabberInterface = GrabberInterface.pyd_field(alias="g")

    def run(self) -> None:
        pass


class FooStage(SampleStage, title="foo-stage"):
    """This is a test stage."""

    field_with_default: str = Field("test", description="Test field with default")
    field_without_default: float = Field(
        alias="fr",
        description="Test field without default",
    )

    def __call__(self, sample: Sample) -> Sample:
        return sample


class ModelWithComplexFieldsTypes(BaseModel, arbitrary_types_allowed=True):
    """Test model with complex fields types."""

    input_folder: InputDatasetInterface
    output_folder: OutputDatasetInterface
    grabber: GrabberInterface
    string: str
    list_of_int: List[int]
    integer: Optional[int]
    float_number: float
    mapping: Dict[str, Any]
    path: Path
    huge_union: Union[
        List[str],
        Dict[int, str],
        Union[bool, float],
        InputDatasetInterface,
        SampleStage,
    ]
    optional_grabber: Optional[GrabberInterface]
    mixed_tuple: Tuple[Sample, int, PipelimeCommand, SampleStage, StageInput, bool]
    debug: bool


@pytest.mark.parametrize(
    "cmd_args,is_needed",
    [
        ({}, True),
        ({"input_folder": "foo"}, True),
        ({"i": "foo"}, True),
        ({"output_folder": "bar"}, True),
        ({"debug": "foo", "grabber": "bar"}, True),
        ({"input_folder": "foo", "output_folder": "bar"}, False),
        ({"i": "foo", "output_folder": "bar"}, False),
        ({"input_folder": "foo", "output_folder": "bar", "debug": "true"}, False),
        (
            {
                "input_folder": "foo",
                "output_folder": "bar",
                "debug": "true",
                "grabber": "foo",
            },
            False,
        ),
        (
            {
                "i": "foo",
                "output_folder": "bar",
                "debug": "true",
                "g": "foo",
            },
            False,
        ),
    ],
)
def test_is_tui_needed_cmd(cmd_args: Dict[str, str], is_needed: bool) -> None:
    assert is_tui_needed(FooCommand, cmd_args) is is_needed


@pytest.mark.parametrize(
    "map_args,is_needed",
    [
        ({}, True),
        ({"input": "foo"}, True),
        ({"i": "foo"}, True),
        ({"output": "bar"}, True),
        ({"o": "bar"}, True),
        ({"input": "foo", "output": "bar"}, True),
        ({"i": "foo", "o": "bar"}, True),
        ({"stage": "unknown"}, False),  # False to let pipelime handle unknown stage
        ({"s": "filter-keys"}, True),
        ({"stage": "filter-keys", "input": "foo", "o": "bar"}, True),
        (
            {
                "i": "foo",
                "output": "bar",
                "s": {"filter-keys": {"negate": "True"}},
            },
            True,
        ),
        (
            {
                "i": "foo",
                "output": "bar",
                "stage": {"filter-keys": {"key_list": ["foo", "bar", "baz"]}},
            },
            False,
        ),
        (
            {
                "i": "foo",
                "output": "bar",
                "s": {
                    "filter-keys": {"key_list": ["foo", "bar", "baz"], "negate": False}
                },
            },
            False,
        ),
    ],
)
def test_is_tui_needed_map(map_args: Dict[str, str], is_needed: bool) -> None:
    assert is_tui_needed(MapCommand, map_args) is is_needed


@pytest.mark.parametrize(
    "stage_args,present",
    [
        ({}, False),
        ({"field_with_default": "foo"}, False),
        ({"field_without_default": "foo"}, True),
        ({"field_with_default": "foo", "field_without_default": "bar"}, True),
        ({"fr": "foo"}, True),
        ({"field_with_default": "foo", "fr": "bar"}, True),
    ],
)
def test_are_stageinput_args_present(stage_args: Dict[str, str], present: bool) -> None:
    assert are_stageinput_args_present(FooStage, stage_args) is present


def test_init_tui_field() -> None:
    fields = [
        FooCommand.__fields__["input_folder"],
        FooCommand.__fields__["output_folder"],
        FooCommand.__fields__["debug"],
        FooCommand.__fields__["grabber"],
    ]

    args = {"i": "foo", "output_folder": "bar"}

    expected_fields = [
        TuiField(
            simple=True,
            name="input_folder",
            description=str(fields[0].field_info.description),
            type_="InputDatasetInterface",
            value="foo",
        ),
        TuiField(
            simple=True,
            name="output_folder",
            description=str(fields[1].field_info.description),
            type_="OutputDatasetInterface",
            value="bar",
        ),
        TuiField(
            simple=True,
            name="debug",
            value="False",
            description=str(fields[2].field_info.description),
            type_="bool",
        ),
        TuiField(
            simple=True,
            name="grabber",
            description=str(fields[3].field_info.description),
            hint=str(FooCommand.__fields__["grabber"].get_default()),
            type_="GrabberInterface",
        ),
    ]

    for field, expected_field in zip(fields, expected_fields):
        assert init_tui_field(field, args) == expected_field


def test_init_tui_stageinput_field() -> None:
    field = MapCommand.__fields__["stage"]
    expected_field = TuiField(
        simple=True,
        name="stage",
        description=str(field.field_info.description),
        type_="StageInput",
    )
    assert init_stageinput_tui_field(field, {}) == expected_field

    stage_info = PipelimeSymbolsHelper.get_stage("format-key")
    stage_info = cast(Tuple[str, str, Type[SampleStage]], stage_info)
    stage_cls = stage_info[-1]

    args = {"stage": "format-key"}
    expected_field = TuiField(
        simple=False,
        name="format-key",
        description=str(stage_cls.__doc__),
        values=[
            init_tui_field(stage_cls.__fields__["key_format"], {}),
            init_tui_field(stage_cls.__fields__["apply_to"], {}),
        ],
    )
    assert init_stageinput_tui_field(field, args) == expected_field

    args = {"s": {"format-key": {"key_format": "suffix"}}}
    stage_args = args["s"]["format-key"]
    expected_field = TuiField(
        simple=False,
        name="format-key",
        description=str(stage_cls.__doc__),
        values=[
            init_tui_field(stage_cls.__fields__["key_format"], stage_args),
            init_tui_field(stage_cls.__fields__["apply_to"], stage_args),
        ],
    )
    assert init_stageinput_tui_field(field, args) == expected_field

    args = {"stage": {"format-key": {"key_format": "my_*_key", "apply_to": "old_key"}}}
    stage_args = args["stage"]["format-key"]
    expected_field = TuiField(
        simple=False,
        name="format-key",
        description=str(stage_cls.__doc__),
        values=[
            init_tui_field(stage_cls.__fields__["key_format"], stage_args),
            init_tui_field(stage_cls.__fields__["apply_to"], stage_args),
        ],
    )
    assert init_stageinput_tui_field(field, args) == expected_field


def test_parse_value() -> None:
    assert parse_value("") == ""

    assert parse_value("foo") == "foo"
    assert parse_value("1") == 1
    assert parse_value("16.0") == 16.0
    assert parse_value("True") is True
    assert parse_value("true") is True
    assert parse_value("False") is False
    assert parse_value("false") is False

    assert parse_value("None") is None
    assert parse_value("none") is None
    assert parse_value("Null") is None
    assert parse_value("null") is None
    assert parse_value("Nul") is None
    assert parse_value("nul") is None

    assert parse_value("[1, 2, 3]") == [1, 2, 3]
    assert parse_value("(1, 2, 3)") == (1, 2, 3)
    assert parse_value("{1, 2, 3}") == {1, 2, 3}
    assert parse_value("{1: 2, 3: 4}") == {1: 2, 3: 4}
    assert parse_value("{'foo': 'bar'}") == {"foo": "bar"}
    assert parse_value("{foo: bar}") == {"foo": "bar"}


def test_get_field_type() -> None:
    fields = ModelWithComplexFieldsTypes.__fields__

    assert get_field_type(fields["input_folder"]) == "InputDatasetInterface"
    assert get_field_type(fields["output_folder"]) == "OutputDatasetInterface"
    assert get_field_type(fields["grabber"]) == "GrabberInterface"

    assert get_field_type(fields["string"]) == "str"
    assert get_field_type(fields["list_of_int"]) == "List[int]"
    assert get_field_type(fields["integer"]) == "Optional[int]"
    assert get_field_type(fields["float_number"]) == "float"
    assert get_field_type(fields["mapping"]) == "Dict[str, Any]"
    assert get_field_type(fields["path"]) == "Path"

    huge_union_type = (
        "Union[List[str], Dict[int, str], bool, float, "
        "InputDatasetInterface, SampleStage]"
    )
    assert get_field_type(fields["huge_union"]) == huge_union_type

    assert get_field_type(fields["optional_grabber"]) == "Optional[GrabberInterface]"

    mixed_tuple_type = (
        "Tuple[Sample, int, PipelimeCommand, SampleStage, StageInput, bool]"
    )
    assert get_field_type(fields["mixed_tuple"]) == mixed_tuple_type

    assert get_field_type(fields["debug"]) == "bool"


def test_tui_init_fields() -> None:
    args = {"input": "foo", "o": "bar", "stage": "remap-key"}

    expected_fields = {
        "stage": TuiField(
            simple=False,
            name="remap-key",
            description=str(StageRemap.__doc__),
            values=[
                TuiField(
                    simple=True,
                    name="remap",
                    description=str(
                        StageRemap.__fields__["remap"].field_info.description
                    ),
                    type_="Mapping[str, str]",
                    value="None",
                ),
                TuiField(
                    simple=True,
                    name="remove_missing",
                    description=str(
                        StageRemap.__fields__["remove_missing"].field_info.description
                    ),
                    type_="bool",
                    value="True",
                ),
            ],
        ),
        "input": TuiField(
            simple=True,
            name="input",
            description=str(MapCommand.__fields__["input"].field_info.description),
            type_="InputDatasetInterface",
            value="foo",
        ),
        "output": TuiField(
            simple=True,
            name="output",
            description=str(MapCommand.__fields__["output"].field_info.description),
            type_="OutputDatasetInterface",
            value="bar",
        ),
        "grabber": TuiField(
            simple=True,
            name="grabber",
            description=str(MapCommand.__fields__["grabber"].field_info.description),
            hint=str(MapCommand.__fields__["grabber"].get_default()),
            type_="GrabberInterface",
        ),
    }

    app = TuiApp(MapCommand, args)
    tui_fields = app.init_fields(args)

    for key, field in tui_fields.items():
        assert key in expected_fields
        assert field == expected_fields[key]


# def test_create_title() -> None:
#     app = TuiApp(FooCommand, {})
#     labels = app.create_title()
#     assert len(labels) == 2

#     title = FooCommand.schema()["title"]
#     assert title in cast(str, labels[0].render())

#     description = FooCommand.schema()["description"]
#     description = fill(
#         description,
#         width=79,
#         replace_whitespace=False,
#         tabsize=4,
#     )
#     assert description in cast(str, labels[1].render())


# def test_create_field() -> None:
#     app = TuiApp(FooCommand, {"i": "foo", "output_folder": "bar"})
#     schema = FooCommand.schema(by_alias=False)

#     defaults = {
#         "input_folder": "foo",
#         "output_folder": "bar",
#         "debug": "False",
#         "grabber": "",
#     }

#     for f in ["input_folder", "output_folder", "debug", "grabber"]:
#         field_info = schema["properties"][f]
#         labels, input_ = app.create_field(f, field_info)
#         assert len(labels) == 2

#         assert f in cast(str, labels[0].render())

#         description = fill(
#             field_info["description"],
#             width=79,
#             replace_whitespace=False,
#             tabsize=4,
#         )
#         assert description in cast(str, labels[1].render())

#         assert input_.value == defaults[f]


# def test_tui_ctrl_c() -> None:
#     async def task() -> None:
#         app = TuiApp(FooCommand, {})
#         async with app.run_test() as pilot:
#             # press "ctrl+c" to abort
#             await pilot.press(Keys.ControlC)

#     with pytest.raises(KeyboardInterrupt):
#         asyncio.run(task())


# @pytest.mark.asyncio
# async def test_tui() -> None:
#     app = TuiApp(FooCommand, {"i": "foo", "output_folder": "bar"})

#     async with app.run_test() as pilot:
#         # add "/path" after "foo" in input_folder input box
#         await pilot.press("/", "p", "a", "t", "h")

#         # move to debug input box
#         await pilot.press(Keys.Tab)
#         await pilot.press(Keys.Tab)

#         # change debug value to "true"
#         for _ in "False":
#             await pilot.press(Keys.Backspace)
#         await pilot.press("t", "r", "u", "e")

#         # press "ctrl+n" to confirm and exit
#         await pilot.press(Keys.ControlN)

#         assert app.cmd_args["input_folder"] == "foo/path"
#         assert app.cmd_args["output_folder"] == "bar"
#         assert app.cmd_args["debug"] == "true"
#         assert app.cmd_args["grabber"] == ""
