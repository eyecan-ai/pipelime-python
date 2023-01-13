from pipelime.cli.utils import (
    print_command_op_stage_info,
    print_commands_ops_stages_list,
    pl_print,
)

import typing as t


class _CallerHelper:
    @staticmethod
    def get_my_caller():
        import inspect

        return inspect.stack()[2]

    @staticmethod
    def get_module(caller) -> str:
        return caller.frame.f_globals["__name__"]

    @staticmethod
    def get_package(caller_module) -> str:
        return caller_module.partition(".")[0].capitalize()

    @staticmethod
    def get_docstring(caller) -> t.Optional[str]:
        import inspect

        return inspect.getdoc(caller.frame.f_globals.get(caller.function))


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
        import inspect

        caller = _CallerHelper.get_my_caller()
        caller_module: str = _CallerHelper.get_module(caller)

        if app_name is None:
            app_name = _CallerHelper.get_package(caller_module)

        if app_description is None:
            app_description = _CallerHelper.get_docstring(caller)

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
    from pathlib import Path

    caller = _CallerHelper.get_my_caller()
    caller_path = Path(caller.filename)

    app = PipelimeApp(
        caller_path.as_posix(),
        app_name=caller_path.stem,
        entry_point=f"python {caller_path.name}",
        app_description=_CallerHelper.get_docstring(caller),
        app_version="<not set>",
    )
    app()
