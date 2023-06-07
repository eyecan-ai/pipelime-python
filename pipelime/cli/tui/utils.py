from typing import Dict, Type

from pipelime.piper import PipelimeCommand


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
