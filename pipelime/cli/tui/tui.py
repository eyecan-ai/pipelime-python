from dataclasses import dataclass
from textwrap import fill
from typing import Dict, List, Mapping, Type

from textual.app import App, ComposeResult
from textual.keys import Keys
from textual.widget import Widget
from textual.widgets import Footer, Input, Label

from pipelime.cli.tui.utils import (
    TuiField,
    init_stageinput_tui_field,
    init_tui_field,
    parse_value,
)
from pipelime.piper import PipelimeCommand
from pipelime.stages import StageInput


@dataclass(frozen=True)
class Constants:
    """Constants used by the TUI."""

    MAX_WIDTH = 100
    SUB_FIELD_MARGIN = (0, 0, 0, 4)


class TuiApp(App[Mapping]):
    """A Textual app to handle Pipelime configurations."""

    CSS_PATH = "tui.css"
    BINDINGS = [
        (Keys.ControlN, "exit", "Confirm"),
        (Keys.ControlS, "save", "Save to file"),
        (Keys.ControlC, "ctrl_c", "Abort"),
    ]

    def __init__(
        self,
        cmd_cls: Type[PipelimeCommand],
        cmd_args: Mapping,
    ) -> None:
        """Create a new TUI app.

        Args:
            cmd_cls: The pipelime command class.
            cmd_args: The args provided by the user (if any).
        """
        super().__init__()
        self.cmd_cls = cmd_cls
        self.fields = self.init_fields(cmd_args)
        self.inputs: Dict[str, Input] = {}

    def init_fields(self, cmd_args: Mapping) -> Dict[str, TuiField]:
        """Initialize the TUI fields.

        Look inside the command pydantic model to find the fields, populating
        them with the default values or the ones provided by the user.

        Args:
            cmd_args: The args provided by the user (if any).

        Returns:
            The TUI fields possibly initialized with default values.
        """
        tui_fields = {}

        for field in self.cmd_cls.__fields__.values():
            if field.type_ == StageInput:
                tui_fields[field.name] = init_stageinput_tui_field(field, cmd_args)
            else:
                tui_fields[field.name] = init_tui_field(field, cmd_args)

        return tui_fields

    def create_title(self) -> List[Label]:
        """Create the title label using the command title and description.

        Returns:
            A list of labels for the title.
        """
        title = self.cmd_cls.command_title()
        description = self.cmd_cls.__doc__
        labels = []
        if title:
            title = TuiApp.preprocess_string(title)
            labels.append(Label(title, classes="title-label"))
        if description:
            description = TuiApp.preprocess_string(description)
            labels.append(Label(description, classes="title-label"))
        return labels

    def create_simple_field(self, field: TuiField) -> List[Widget]:
        """Create labels and input box for a simple field.

        Args:
            field: The field.

        Returns:
            A list of widgets containing the labels and the input box.
        """
        widgets: List[Widget] = []

        title = TuiApp.preprocess_string(field.name)
        label = Label(title, classes="field-label")
        widgets.append(label)

        description = field.description
        descr = f"({field.type_}) {description}" if description else f"({field.type_})"
        description = TuiApp.preprocess_string(descr)
        widgets.append(Label(description))

        default = str(field.value)
        inp = Input(value=default)
        widgets.append(inp)
        self.inputs[field.name] = inp

        return widgets

    def create_dict_field(self, field: TuiField) -> List[Widget]:
        """Create labels and input boxes for a dictionary field.

        Args:
            field: The dictionary field.

        Returns:
            A list with all the needed widgets.
        """
        widgets: List[Widget] = []

        title = field.name + f" ({field.type_})"
        title = TuiApp.preprocess_string(title)
        label = Label(title, classes="field-label")
        widgets.append(label)

        description = field.description
        if description:
            description = TuiApp.preprocess_string(description)
            widgets.append(Label(description))

        for sub_field in field.values:
            sub_widgets = self.create_simple_field(sub_field)
            for widget in sub_widgets:
                widget.styles.margin = Constants.SUB_FIELD_MARGIN
                widgets.append(widget)

        return widgets

    def compose(self) -> ComposeResult:
        """Compose the TUI."""

        title_labels = self.create_title()
        for label in title_labels:
            yield label
        yield Label(" ")

        for field in self.fields.values():
            if field.simple:
                widgets = self.create_simple_field(field)
            else:
                widgets = self.create_dict_field(field)

            for widget in widgets:
                yield widget
            yield Label(" ")

        yield Footer()

    def action_exit(self) -> None:
        """Exit the TUI.

        Collect the values from the input boxes and exit the TUI.
        """
        cmd_args = {}

        for f, field in self.fields.items():
            if field.simple:
                value = parse_value(self.inputs[field.name].value)
                cmd_args[f] = value
            else:
                cmd_args[f] = {field.name: {}}
                for sub_f in field.values:
                    value = parse_value(self.inputs[sub_f.name].value)
                    cmd_args[f][field.name][sub_f.name] = value

        self.exit(cmd_args)

    def action_save(self) -> None:
        """Save the current configuration."""

        pass

    def action_ctrl_c(self) -> None:
        """Propagate the KeyboardInterrupt exception."""

        raise KeyboardInterrupt

    @staticmethod
    def preprocess_string(s: str) -> str:
        subs = s.split("\n")
        preprocessed = ""

        for sub in subs:
            sub = fill(
                sub,
                width=Constants.MAX_WIDTH,
                replace_whitespace=False,
                tabsize=4,
            )
            sub = sub.replace("[", "\[")
            preprocessed += sub + "\n"

        preprocessed = preprocessed[:-1]

        return preprocessed
