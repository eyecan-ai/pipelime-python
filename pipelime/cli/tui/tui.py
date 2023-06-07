import json
from ast import literal_eval
from json import JSONDecodeError
from textwrap import fill
from typing import Any, Dict, List, Tuple, Type

import yaml
from textual.app import App, ComposeResult
from textual.keys import Keys
from textual.widget import Widget
from textual.widgets import Footer, Input, Label
from yaml.error import YAMLError

from pipelime.cli.tui.utils import (
    TuiField,
    init_field,
    init_stageinput_field,
    is_stageinput,
)
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
        self.fields = self.init_fields(cmd_args)
        self.inputs: Dict[str, Input] = {}

    def init_fields(self, cmd_args: Dict[str, Any]) -> Dict[str, TuiField]:
        """Initialize the command fields.

        Look inside the command schema to find the required fields,
        populating them with the default values or the ones provided
        by the user.

        Args:
            cmd_args: The args provided by the user (if any).

        Returns:
            The command fields possibly initialized with default values.
        """
        args = {}

        fields = self.cmd_schema["properties"]
        for f in fields:
            field_info = fields[f]

            if is_stageinput(field_info):
                args[f] = init_stageinput_field(f, field_info, cmd_args)
            else:
                args[f] = init_field(f, field_info, cmd_args)

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

    def create_simple_field(self, field: TuiField) -> List[Widget]:
        """Create labels and input box for a field.

        Args:
            field: The field.

        Returns:
            A tuple containing the labels and the input box.
        """
        widgets: List[Widget] = [Label(field.title, classes="field-label")]

        description = field.description
        if description:
            description = fill(
                description,
                width=79,
                replace_whitespace=False,
                tabsize=4,
            )
            widgets.append(Label(description))

        default = str(field.value)
        inp = Input(value=default)
        widgets.append(inp)
        self.inputs[field.title] = inp

        return widgets

    def create_dict_field(self, field: TuiField) -> List[Widget]:
        """Create labels and input box for a dictionary field.

        Args:
            field_name: The name of the field.

        Returns:
            A tuple containing the labels and the input box.
        """
        widgets: List[Widget] = [Label(field.title, classes="field-label")]

        description = field.description
        if description:
            description = fill(
                description,
                width=79,
                replace_whitespace=False,
                tabsize=4,
            )
            widgets.append(Label(description))

        for f in field.value:
            f_widgets = self.create_simple_field(f)
            for f_widget in f_widgets:
                f_widget.styles.margin = (0, 0, 0, 2)
                widgets.append(f_widget)
            # self.inputs[field.title] = inp

        return widgets

    def compose(self) -> ComposeResult:
        """Compose the TUI."""
        title_labels = self.create_title()
        for label in title_labels:
            yield label
        yield Label(" ")

        for f in self.fields:
            if self.fields[f].simple:
                widgets = self.create_simple_field(self.fields[f])
            else:
                widgets = self.create_dict_field(self.fields[f])

            for widget in widgets:
                yield widget
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
