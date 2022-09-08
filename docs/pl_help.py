import os
from contextlib import redirect_stdout
from pathlib import Path
import rich
from rich.console import Console

from pipelime.cli import print_commands_ops_stages_list

os.environ["COLUMNS"] = "100"

base_path = Path(__file__).parent.resolve() / "_static/generated"
base_path.mkdir(parents=True, exist_ok=True)

rich.reconfigure(record=True)

with open(os.devnull, "w", encoding="utf-8") as f, redirect_stdout(f):
    print_commands_ops_stages_list(
        True, show_cmds=True, show_ops=False, show_stages=False
    )

with (base_path / "pl_cmds.html").open("w", encoding="utf-8") as f:
    f.write(rich.get_console().export_html())

with open(os.devnull, "w", encoding="utf-8") as f, redirect_stdout(f):
    print_commands_ops_stages_list(
        True, show_cmds=False, show_ops=False, show_stages=True
    )

with (base_path / "pl_stgs.html").open("w", encoding="utf-8") as f:
    f.write(rich.get_console().export_html())

with open(os.devnull, "w", encoding="utf-8") as f, redirect_stdout(f):
    print_commands_ops_stages_list(
        True, show_cmds=False, show_ops=True, show_stages=False
    )

with (base_path / "pl_ops.html").open("w", encoding="utf-8") as f:
    f.write(rich.get_console().export_html())
