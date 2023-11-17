from pathlib import Path
from typing import Any, Dict, Optional

from pipelime.choixe.visitors.processor import ChoixeProcessingError, Processor
from pipelime.cli.utils import get_user_input


class ProcessorUi(Processor):
    """Processor that asks the user to fill the variables missing in the context."""

    def __init__(
        self,
        context: Optional[Dict[str, Any]] = None,
        cwd: Optional[Path] = None,
        allow_branching: bool = True,
        ask_missing_vars: bool = False,
    ) -> None:
        """Constructor for `ProcessorUi`

        Args:
            context (Optional[Dict[str, Any]], optional): A data structure containing
                the values that will replace the variable nodes. Defaults to None.
            cwd (Optional[Path], optional): current working directory used for relative
                imports. If set to None, the `os.getcwd()` will be used.
                Defaults to None.
            allow_branching (bool, optional): Set to False to disable processing on
                branching nodes, like sweeps. All branching nodes will be simply
                unparsed. Defaults to True.
            ask_missing_vars (bool, optional): Set to True to allow the user to fill
                the variables missing in the context.
        """
        super().__init__(context, cwd, allow_branching)
        self._ask_missing_vars = ask_missing_vars
        self._user_defined_vars: Dict[str, Any] = {}

    def _handle_missing_var(self, varname: str) -> Any:
        if self._ask_missing_vars:
            msg = f"Enter value for [yellow]{varname}[/yellow]"
            value = get_user_input(msg)
            self._user_defined_vars[varname] = value
            self._context[varname] = value
            return value
        else:
            raise ChoixeProcessingError(f"Variable not found: `{varname}`")

    def _handle_missing_for(self, varname: str) -> Any:
        if self._ask_missing_vars:
            msg = f"Enter value for [yellow]{varname}[/yellow]"
            iterable = get_user_input(msg)
            self._user_defined_vars[varname] = iterable
            self._context[varname] = iterable
            return iterable
        else:
            raise ChoixeProcessingError(f"Loop variable `{varname}` not found")

    def _handle_missing_switch(self, varname: str) -> Any:
        if self._ask_missing_vars:
            msg = f"Enter value for [yellow]{varname}[/yellow]"
            value = get_user_input(msg)
            self._user_defined_vars[varname] = value
            self._context[varname] = value
            return value
        else:
            raise ChoixeProcessingError(f"Switch variable `{varname}` not found")
