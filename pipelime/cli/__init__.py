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
        app_version: t.Optional[str] = None,
        **fixed_cmdline_args,
    ):
        import inspect

        caller_module = inspect.stack()[1].frame.f_globals["__name__"]
        package_name = caller_module.partition(".")[0]

        self._app_name = app_name or package_name.capitalize()
        self._entry_point = entry_point
        self._app_version = app_version

        self._extra_modules = extra_modules if extra_modules else [caller_module]
        self._fixed_cmdline_args = fixed_cmdline_args

    def __call__(self):
        from pipelime.cli.main import run_with_extra_modules

        run_with_extra_modules(
            app_name=self._app_name,
            entry_point=self._entry_point,
            version=self._app_version,
            extra_modules=self._extra_modules,
            fixed_cmdline_args=self._fixed_cmdline_args,
        )


def run():
    import inspect
    from pathlib import Path

    caller_module = Path(inspect.stack()[1].filename)

    app = PipelimeApp(
        caller_module.as_posix(),
        app_name=caller_module.stem,
        entry_point=f"python {caller_module.name}",
        app_version="<not set>",
    )
    app()
