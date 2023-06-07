import asyncio
from textwrap import fill
from typing import Dict, cast

import pytest
from pydantic import Field
from textual.keys import Keys

from pipelime.cli.tui import TuiApp, is_tui_needed
from pipelime.commands.interfaces import (
    GrabberInterface,
    InputDatasetInterface,
    OutputDatasetInterface,
)
from pipelime.piper import PipelimeCommand, PiperPortType


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
def test_is_tui_needed(cmd_args: Dict[str, str], is_needed: bool) -> None:
    assert is_tui_needed(FooCommand, cmd_args) is is_needed


@pytest.mark.parametrize(
    "cmd_args,parsed_args",
    [
        (
            {},
            {
                "input_folder": "",
                "output_folder": "",
                "debug": "False",
                "grabber": "",
            },
        ),
        (
            {"input_folder": "foo"},
            {
                "input_folder": "foo",
                "output_folder": "",
                "debug": "False",
                "grabber": "",
            },
        ),
        (
            {"i": "foo"},
            {
                "input_folder": "foo",
                "output_folder": "",
                "debug": "False",
                "grabber": "",
            },
        ),
        (
            {"output_folder": "bar"},
            {
                "input_folder": "",
                "output_folder": "bar",
                "debug": "False",
                "grabber": "",
            },
        ),
        (
            {"debug": "foo", "grabber": "bar"},
            {
                "input_folder": "",
                "output_folder": "",
                "debug": "foo",
                "grabber": "bar",
            },
        ),
        (
            {"input_folder": "foo", "output_folder": "bar"},
            {
                "input_folder": "foo",
                "output_folder": "bar",
                "debug": "False",
                "grabber": "",
            },
        ),
        (
            {"i": "foo", "output_folder": "bar"},
            {
                "input_folder": "foo",
                "output_folder": "bar",
                "debug": "False",
                "grabber": "",
            },
        ),
        (
            {"input_folder": "foo", "output_folder": "bar", "debug": "true"},
            {
                "input_folder": "foo",
                "output_folder": "bar",
                "debug": "true",
                "grabber": "",
            },
        ),
        (
            {
                "input_folder": "foo",
                "output_folder": "bar",
                "debug": "true",
                "grabber": "duh",
            },
            {
                "input_folder": "foo",
                "output_folder": "bar",
                "debug": "true",
                "grabber": "duh",
            },
        ),
        (
            {
                "i": "foo",
                "output_folder": "bar",
                "debug": "true",
                "g": "duh",
            },
            {
                "input_folder": "foo",
                "output_folder": "bar",
                "debug": "true",
                "grabber": "duh",
            },
        ),
        (
            {
                "i": "foo",
                "output_folder": "bar",
                "debug": "true",
                "g": "duh",
                "extra_key": "extra_value",
            },
            {
                "input_folder": "foo",
                "output_folder": "bar",
                "debug": "true",
                "grabber": "duh",
                "extra_key": "extra_value",
            },
        ),
    ],
)
def test_init_args(cmd_args: Dict[str, str], parsed_args: Dict[str, str]) -> None:
    app = TuiApp(FooCommand, cmd_args)
    assert app.cmd_args == parsed_args


def test_create_title() -> None:
    app = TuiApp(FooCommand, {})
    labels = app.create_title()
    assert len(labels) == 2

    title = FooCommand.schema()["title"]
    assert title in cast(str, labels[0].render())

    description = FooCommand.schema()["description"]
    description = fill(
        description,
        width=79,
        replace_whitespace=False,
        tabsize=4,
    )
    assert description in cast(str, labels[1].render())


def test_create_field() -> None:
    app = TuiApp(FooCommand, {"i": "foo", "output_folder": "bar"})
    schema = FooCommand.schema(by_alias=False)

    defaults = {
        "input_folder": "foo",
        "output_folder": "bar",
        "debug": "False",
        "grabber": "",
    }

    for f in ["input_folder", "output_folder", "debug", "grabber"]:
        field_info = schema["properties"][f]
        labels, input_ = app.create_field(f, field_info)
        assert len(labels) == 2

        assert f in cast(str, labels[0].render())

        description = fill(
            field_info["description"],
            width=79,
            replace_whitespace=False,
            tabsize=4,
        )
        assert description in cast(str, labels[1].render())

        assert input_.value == defaults[f]


def test_tui_ctrl_c() -> None:
    async def task() -> None:
        app = TuiApp(FooCommand, {})
        async with app.run_test() as pilot:
            # press "ctrl+c" to abort
            await pilot.press(Keys.ControlC)

    with pytest.raises(KeyboardInterrupt):
        asyncio.run(task())


@pytest.mark.asyncio
async def test_tui() -> None:
    app = TuiApp(FooCommand, {"i": "foo", "output_folder": "bar"})

    async with app.run_test() as pilot:
        # add "/path" after "foo" in input_folder input box
        await pilot.press("/", "p", "a", "t", "h")

        # move to debug input box
        await pilot.press(Keys.Tab)
        await pilot.press(Keys.Tab)

        # change debug value to "true"
        for _ in "False":
            await pilot.press(Keys.Backspace)
        await pilot.press("t", "r", "u", "e")

        # press "ctrl+n" to confirm and exit
        await pilot.press(Keys.ControlN)

        assert app.cmd_args["input_folder"] == "foo/path"
        assert app.cmd_args["output_folder"] == "bar"
        assert app.cmd_args["debug"] == "true"
        assert app.cmd_args["grabber"] == ""
