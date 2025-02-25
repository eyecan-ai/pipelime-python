from dataclasses import dataclass
from pathlib import Path
from textwrap import fill
from typing import Dict, List, Mapping, Type, cast

import yaml
from textual.app import App, ComposeResult
from textual.containers import Container
from textual.keys import Keys
from textual.screen import ModalScreen
from textual.widget import Widget
from textual.widgets import Footer, Input, Label

from pipelime.choixe import Choixe
from pipelime.cli.tui.utils import (
    TuiField,
    init_stageinput_tui_field,
    init_tui_field,
)
from pipelime.cli.utils import parse_user_input
from pipelime.piper import PipelimeCommand
from pipelime.stages import StageInput


@dataclass(frozen=True)
class Constants:
    """Constants used by the TUI."""

    MAX_STRING_WIDTH = 100
    SUB_FIELD_MARGIN = (0, 0, 0, 4)
    TUI_KEY_CONFIRM = Keys.ControlN
    TUI_KEY_SAVE = Keys.ControlS
    TUI_KEY_ABORT = Keys.ControlB
    TUI_KEY_TOGGLE_DESCRIPTIONS = Keys.ControlJ
    SAVE_KEY_CONFIRM = Keys.ControlS
    SAVE_KEY_CANCEL = Keys.Escape


class SaveScreen(ModalScreen):
    BINDINGS = [
        (Constants.SAVE_KEY_CONFIRM, "confirm", "Confirm"),
        (Constants.SAVE_KEY_CANCEL, "cancel", "Cancel"),
    ]
    HELP = "CTRL+S to confirm, ESC to cancel"

    def __init__(self, config: Mapping) -> None:
        """Create a new save screen.

        Args:
            config: The configuration to save.
        """
        super().__init__()
        self.config = config

    def compose(self) -> ComposeResult:
        """Compose the save screen."""
        error_label = Label("", classes="error-label")
        error_label.display = False
        yield Container(
            Label("Save path", classes="field-label"),
            Input(""),
            Label(str(SaveScreen.HELP)),
            error_label,
            id="dialog",
        )

    def action_confirm(self) -> None:
        """Confirm the save path."""
        path = self.query_one(Input).value

        try:
            if path == "":
                raise ValueError("Path cannot be empty.")
            elif Path(path).is_dir():
                raise FileExistsError(f"'{path}' is a directory.")
            elif Path(path).exists():
                raise FileExistsError(f"'{path}' already exists.")

            config = Choixe(self.config)
            config = config.decode()

            with open(path, "w") as f:
                yaml.dump(config, f)
            self.app.pop_screen()

        except Exception as e:
            error_label = self.query_one(".error-label")
            error_label = cast(Label, error_label)
            error_label.display = True
            error = str(e)
            # escape the square brackets to avoid rich syntax
            error = error.replace("[", r"\[")  # noqa: W605
            error_label.update(error)

    def action_cancel(self) -> None:
        """Cancel the save."""
        self.app.pop_screen()


class TuiApp(App[Mapping]):
    """A Textual app to handle Pipelime configurations."""

    CSS_PATH = "tui.css"
    BINDINGS = [
        (Constants.TUI_KEY_CONFIRM, "confirm", "Confirm"),
        (Constants.TUI_KEY_SAVE, "save", "Save to file"),
        (Constants.TUI_KEY_ABORT, "abort", "Abort"),
        (
            Constants.TUI_KEY_TOGGLE_DESCRIPTIONS,
            "toggle_descriptions",
            "Show/hide descriptions",
        ),
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
        self.show_descriptions = False

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
            label = Label(description, classes="title-label description")
            label.display = True if self.show_descriptions else False
            labels.append(label)
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
        label = Label(description, classes="description")
        label.display = True if self.show_descriptions else False
        widgets.append(label)

        inp = Input(value=field.value, placeholder=field.hint)
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

        title = field.name
        title = TuiApp.preprocess_string(title)
        label = Label(title, classes="field-label")
        widgets.append(label)

        description = field.description
        if description:
            description = TuiApp.preprocess_string(description)
            widgets.append(Label(description, classes="description"))

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

    def collect_cmd_args(self) -> Mapping:
        """Collect the command args from the input boxes.

        Returns:
            The command args.
        """
        cmd_args = {}

        for f, field in self.fields.items():
            if field.simple:
                value = parse_user_input(self.inputs[field.name].value)
                cmd_args[f] = value
            else:
                cmd_args[f] = {field.name: {}}
                for sub_f in field.values:
                    value = parse_user_input(self.inputs[sub_f.name].value)
                    cmd_args[f][field.name][sub_f.name] = value

        return cmd_args

    def action_confirm(self) -> None:
        """Exit the TUI.

        Collect the values from the input boxes and exit the TUI.
        """
        cmd_args = self.collect_cmd_args()
        self.exit(cmd_args)

    def action_save(self) -> None:
        """Save the current configuration."""
        save_screen = SaveScreen(self.collect_cmd_args())
        self.push_screen(save_screen)

    def action_abort(self) -> None:
        """Raise KeyboardInterrupt exception."""
        raise KeyboardInterrupt

    def action_toggle_descriptions(self) -> None:
        """Hide/show all the descriptions."""
        self.show_descriptions = not self.show_descriptions
        query = self.query(".description")
        for widget in query:
            widget.display = True if self.show_descriptions else False

        query = self.query(Input)
        for widget in query:
            if widget.has_focus:
                widget.scroll_visible()

    @staticmethod
    def preprocess_string(s: str) -> str:
        """Preprocess a string to be displayed in the TUI.

        The input string is first split according to the newlines, then each
        substring is wrapped to a maximum width and finally the square brackets
        are escaped to avoid rich syntax.

        Args:
            s: The string to preprocess.

        Returns:
            The preprocessed string.
        """
        subs = s.split("\n")
        preprocessed = ""

        for sub in subs:
            sub = fill(
                sub,
                width=Constants.MAX_STRING_WIDTH,
                replace_whitespace=False,
                tabsize=4,
            )
            sub = sub.replace("[", r"\[")  # noqa: W605
            preprocessed += sub + "\n"

        preprocessed = preprocessed[:-1]

        return preprocessed
