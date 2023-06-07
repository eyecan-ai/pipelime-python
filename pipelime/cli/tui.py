import json
from ast import literal_eval
from json import JSONDecodeError
from textwrap import fill
from typing import Any, Dict, List, Tuple, Type

import yaml
from textual.app import App, ComposeResult
from textual.keys import Keys
from textual.widgets import Footer, Input, Label
from yaml.error import YAMLError

from pipelime.piper import PipelimeCommand


class TuiApp(App[Dict[str, str]]):
    """A Textual app to handle Pipelime configurations."""

    CSS_PATH = "tui.css"
    BINDINGS = [
        (Keys.ControlN, "exit", "Confirm and exit"),
        (Keys.ControlC, "ctrl_c", ""),
    ]

    def __init__(
        self,
        cmd_cls: Type[PipelimeCommand],
        cmd_args: Dict[str, str],
    ) -> None:
        """Create a new TUI app.

        Args:
            cmd_cls: The pipelime command class.
            cmd_args: The args provided by the user (if any).
        """
        super().__init__()
        self.cmd_schema = cmd_cls.schema(by_alias=False)
        self.cmd_args = self.init_args(cmd_args)
        self.inputs: Dict[str, Input] = {}

    def init_args(self, cmd_args: Dict[str, Any]) -> Dict[str, str]:
        """Initialize the command arguments.

        Look inside the command schema to find the required fields,
        populating them with the default values or the ones provided
        by the user.

        Args:
            cmd_args: The args provided by the user (if any).

        Returns:
            The command arguments possibly initialized with default values.
        """
        args = {}

        fields = self.cmd_schema["properties"]
        for f in fields:
            alias = fields[f].get("title", "").lower()
            if f in cmd_args:
                args[f] = str(cmd_args.pop(f))
            elif alias.lower() in cmd_args:
                args[f] = str(cmd_args.pop(alias))
            else:
                default = fields[f].get("default", "")
                args[f] = str(default)

        args.update(cmd_args)

        return args

    def create_title(self) -> List[Label]:
        """Create the title label using the command title and description.

        Returns:
            A list of labels for the tile.
        """
        title = self.cmd_schema.get("title", "")
        description = self.cmd_schema.get("description", "")
        labels = []
        if title:
            labels.append(Label(title, classes="title-label"))
        if description:
            description = fill(
                description,
                width=79,
                replace_whitespace=False,
                tabsize=4,
            )
            labels.append(Label(description, classes="title-label"))
        return labels

    def create_field(
        self,
        name: str,
        field_info: Dict[str, Any],
    ) -> Tuple[List[Label], Input]:
        """Create labels and input box for a field.

        Args:
            name: The name of the field.
            field_info: The field info from the command schema.

        Returns:
            A tuple containing the labels and the input box.
        """
        labels = [Label(name, classes="field-label")]
        description = field_info.get("description", "")
        if description:
            description = fill(
                description,
                width=79,
                replace_whitespace=False,
                tabsize=4,
            )
            labels.append(Label(description))

        default = self.cmd_args[name]
        inp = Input(value=default)
        self.inputs[name] = inp

        return labels, inp

    def compose(self) -> ComposeResult:
        """Compose the TUI."""
        title_labels = self.create_title()
        for label in title_labels:
            yield label
        yield Label(" ")

        fields = self.cmd_schema["properties"]
        for f in fields:
            labels, inp = self.create_field(f, fields[f])

            for label in labels:
                yield label
            yield inp
            yield Label(" ")

        yield Footer()

    def action_exit(self) -> None:
        """Exit the TUI.

        Collect the values from the input boxes and exit the TUI.
        """
        for inp in self.inputs:
            value = self.inputs[inp].value

            if value:
                parse_fns = [yaml.safe_load, json.loads, literal_eval]
                parsed = False

                while not parsed and parse_fns:
                    try:
                        value = parse_fns.pop(0)(value)
                        parsed = True
                    except (YAMLError, JSONDecodeError, ValueError, SyntaxError):
                        pass

            self.cmd_args[inp] = value

        self.exit(self.cmd_args)

    def action_ctrl_c(self) -> None:
        """Propagate the KeyboardInterrupt exception."""
        raise KeyboardInterrupt


def is_tui_needed(cmd_cls: Type[PipelimeCommand], cmd_args: Dict[str, str]) -> bool:
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

    for f in fields:
        alias = fields[f].get("title", "").lower()
        if (f not in cmd_args) and (alias.lower() not in cmd_args) and (f in required):
            return True

    return False
