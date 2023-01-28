from pipelime.cli.utils import (
    print_command_op_stage_info,
    print_commands_ops_stages_list,
    pl_print,
)

import typing as t


class PipelimeApp:
    def __init__(
        self,
        *extra_modules: str,
        app_name: t.Optional[str] = None,
        entry_point: t.Optional[str] = None,
        app_description: t.Optional[str] = None,
        app_version: t.Optional[str] = None,
        **extra_args: t.Union[str, t.Sequence[str]],
    ):
        """Create a PipelimeApp instance.

        Args:
            *extra_modules (str): the extra modules to load.
                Defaults to the caller module
            app_name (t.Optional[str], optional): the app name.
                Defaults to the package name.
            entry_point (t.Optional[str], optional): the name of the entry point.
                Defaults to package name.
            app_description (t.Optional[str], optional): a description of the app.
                Defaults to the docstring of the caller.
            app_version (t.Optional[str], optional): the app version.
                Defaults to the Pipelime version.
            **extra_args (str): additional command line arguments. When an argument
                must be specified multiple times, use a list as value.
        """
        from pipelime.utils.inspection import MyCaller

        caller = MyCaller()
        caller_module = caller.module

        if app_name is None:
            app_name = caller.package.capitalize()

        if app_description is None:
            app_description = caller.docstrings

        self._app_name = app_name
        self._entry_point = entry_point
        self._app_description = app_description
        self._app_version = app_version

        extra_modules = extra_modules if extra_modules else (caller_module,)
        self._extra_args = [p for m in extra_modules for p in ("-m", m)]

        for k, v in extra_args.items():
            if isinstance(v, str):
                self._extra_args += [k, v]
            else:
                self._extra_args.extend((p for vv in v for p in (k, vv)))

    def __call__(self):
        from pipelime.cli.main import run_typer_app

        run_typer_app(
            app_name=self._app_name,
            entry_point=self._entry_point,
            app_description=self._app_description,
            version=self._app_version,
            extra_args=self._extra_args,
        )


def run():
    """Runs a default PipelimeApp instance from your module.

    Example:
        ```
        @command
        def my_command():
            ...

        if __name__ == "__main__":
            run()
        ```
    """
    from pipelime.utils.inspection import MyCaller
    from pathlib import Path

    caller = MyCaller()
    caller_path = Path(caller.filename)

    app = PipelimeApp(
        caller.module,
        app_name=caller_path.stem,
        entry_point=f"python {caller_path.name}",
        app_description=caller.docstrings,
        app_version="<not set>",
    )
    app()
