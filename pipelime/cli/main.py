import typing as t
from types import ModuleType

import typer
from rich import print as rprint


def _pinfo(val):
    rprint(f"[cyan]{val}[/]")


def _pwarn(val):
    rprint(f"[orange1][bold blink]WARNING:[/bold blink] {val}[/orange1]")


def _perr(val):
    rprint(
        f"[dark_red on white][bold blink]ERROR:[/bold blink] {val}[/dark_red on white]"
    )


class _Helper:
    std_cmd_modules = ["pipelime.commands"]
    extra_modules: t.List[str] = []

    cached_modules: t.Dict[str, ModuleType] = {}
    cached_cmds: t.Dict[t.Tuple[str, str], t.Dict] = {}
    cached_seq_ops: t.Dict[t.Tuple[str, str], t.Dict] = {}

    @classmethod
    def complete_operator(cls, is_cmd: bool, is_seq_ops: bool):
        import inspect

        valid_completion_items = list(
            (cls.get_piper_commands() if is_cmd else {}).values()
        ) + list((cls.get_sequence_operators() if is_seq_ops else {}).values())
        valid_completion_items = [
            (k, inspect.getdoc(v) or "")
            for elem in valid_completion_items
            for k, v in elem.items()
        ]

        def _complete(incomplete: str):
            for name, help_text in valid_completion_items:
                if name.startswith(incomplete):
                    yield (name, help_text)

        return _complete

    @classmethod
    def set_extra_modules(cls, modules: t.Sequence[str]):
        cls.extra_modules = list(modules)

    @classmethod
    def is_cache_valid(cls) -> bool:
        return all(
            m in cls.cached_modules for m in (cls.std_cmd_modules + cls.extra_modules)
        )

    @classmethod
    def _load_commands(cls):
        import inspect

        from pipelime.piper import PipelimeCommand

        def _warn_double_def(cmd_name, first, second):
            _perr(f"Found duplicate command `{cmd_name}`")
            _pwarn(f"Defined as `{first.classpath()}`")
            _pwarn(f"       and `{second.classpath()}`")
            raise typer.Exit(1)

        all_cmds = {}
        for module_name, module_ in cls.cached_modules.items():
            module_cmds = tuple(
                (cmd_cls.command_title(), cmd_cls)
                for _, cmd_cls in inspect.getmembers(
                    module_,
                    lambda v: inspect.isclass(v)
                    and issubclass(v, PipelimeCommand)
                    and v is not PipelimeCommand,
                )
            )

            if module_name.endswith(".py"):
                for _, cmd_cls in module_cmds:
                    cmd_cls._classpath = f"{module_name}:{cmd_cls.__name__}"

            # check for double commands in the same module
            for idx, (cmd_name, cmd_cls) in enumerate(module_cmds):
                for other_cmd_name, other_cmd_cls in module_cmds[idx + 1 :]:  # noqa
                    if cmd_name == other_cmd_name:
                        _warn_double_def(cmd_name, cmd_cls, other_cmd_cls)

            # check for double commands across modules
            module_cmds = dict(module_cmds)
            for cmd_name, cmd_cls in module_cmds.items():
                if cmd_name in all_cmds:
                    _warn_double_def(cmd_name, cmd_cls, all_cmds[cmd_name])

            all_cmds = {**all_cmds, **module_cmds}
        return all_cmds

    @classmethod
    def import_operators_and_commands(cls):
        import pipelime.choixe.utils.imports as pl_imports
        import pipelime.sequences as pls

        if not cls.is_cache_valid():
            for module_name in cls.std_cmd_modules + cls.extra_modules:
                if module_name not in cls.cached_modules:
                    cls.cached_modules[module_name] = (
                        pl_imports.import_module_from_file(module_name)
                        if module_name.endswith(".py")
                        else pl_imports.import_module_from_path(module_name)
                    )

            cls.cached_seq_ops = {
                (
                    "Sequence Generator",
                    "Sequence Generators",
                ): pls.SamplesSequence._sources,
                (
                    "Sequence Operation",
                    "Sequence Operations",
                ): pls.SamplesSequence._pipes,
            }

            cls.cached_cmds = {
                ("Piper Command", "Piper Commands"): cls._load_commands()
            }

    @classmethod
    def get_sequence_operators(cls):
        cls.import_operators_and_commands()
        return cls.cached_seq_ops

    @classmethod
    def get_piper_commands(cls):
        cls.import_operators_and_commands()
        return cls.cached_cmds

    @classmethod
    def get_operator(cls, operator_name: str):
        for op_type, op_dict in _Helper.get_sequence_operators().items():
            if operator_name in op_dict:
                return (op_type, op_dict[operator_name])
        return None

    @classmethod
    def get_command(cls, command_name: str):
        from pydantic import BaseModel

        import pipelime.choixe.utils.imports as pl_imports
        from pipelime.piper import PipelimeCommand

        if "." in command_name or ":" in command_name:
            try:
                imported_symbol = pl_imports.import_symbol(command_name)
                if not issubclass(imported_symbol, BaseModel):
                    raise ValueError(f"{command_name} is not a pydantic model.")

                return (
                    ("Imported Command", "Imported Commands")
                    if issubclass(imported_symbol, PipelimeCommand)
                    else ("Imported Model", "Imported Models"),
                    imported_symbol,
                )
            except ImportError:
                return None

        for cmd_type, cmd_dict in _Helper.get_piper_commands().items():
            if command_name in cmd_dict:
                return (cmd_type, cmd_dict[command_name])
        return None


def _convert_val(val: str):
    if val.lower() == "true":
        return True
    if val.lower() == "false":
        return False
    if val.lower() in ("none", "null"):
        return None
    try:
        num = int(val)
        return num
    except ValueError:
        pass
    try:
        num = float(val)
        return num
    except ValueError:
        pass
    return val


app = typer.Typer()


@app.callback()
def callback(
    extra_modules: t.List[str] = typer.Option(
        [], "--module", "-m", help="Additional modules to import."
    )
):
    """
    Pipelime Command Line Interface
    """
    _Helper.set_extra_modules(extra_modules)


def _store_opt(last_opt, last_val, all_opts):
    import pydash as py_

    if last_opt is not None:
        curr_val = py_.get(all_opts, last_opt, None)
        if curr_val is None:
            py_.set_(all_opts, last_opt, last_val)
        else:
            if not isinstance(curr_val, list):
                curr_val = [curr_val]
            py_.set_(all_opts, last_opt, curr_val + [last_val])


@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def run(
    command: str = typer.Argument(
        ...,
        help=(
            "The Piper command to run, ie, a `command-name`, "
            "a `package.module.ClassName` class path or "
            "a `path/to/module.py:ClassName` uri."
        ),
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    ctx: typer.Context = typer.Option(None),
):
    """
    Run a piper command.
    """

    from pipelime.piper import PipelimeCommand

    cmd_cls = _Helper.get_command(command)
    if cmd_cls is None or not issubclass(cmd_cls[1], PipelimeCommand):
        _perr(f"{command} is not a piper command!")
        _pwarn("Have you added the module with `--module`?")
        raise typer.Exit(1)
    cmd_cls = cmd_cls[1]

    last_opt = None
    last_val = None
    all_opts = {}

    for extra_arg in ctx.args:
        if extra_arg.startswith("--"):
            _store_opt(last_opt, last_val, all_opts)
            last_opt, _, last_val = extra_arg[2:].partition("=")
            last_val = None if not last_val else _convert_val(last_val)
        elif last_val is None:
            last_val = _convert_val(extra_arg)
        else:
            if not isinstance(last_val, tuple):
                last_val = (last_val,)
            last_val += (_convert_val(extra_arg),)
    _store_opt(last_opt, last_val, all_opts)

    if verbose:
        _pinfo(f"\nCreating command `{command}` with options:")
        _pinfo(all_opts)

    cmd_obj = cmd_cls(**all_opts)

    if verbose:
        _pinfo(f"\nCreated command `{command}`:")
        _pinfo(repr(cmd_obj))

    if verbose:
        _pinfo(f"\nRunning `{command}`...")
    cmd_obj()


def _print_details(info, show_class_path_and_piper_port):
    from pipelime.cli.pretty_print import print_model_info

    for info_type, info_map in info.items():
        for info_cls in info_map.values():
            print(f"\n---{info_type[0]}")
            print_model_info(
                info_cls,
                show_class_path=show_class_path_and_piper_port,
                show_piper_port=show_class_path_and_piper_port,
            )


def _print_short_help(info, show_class_path):
    from pipelime.cli.pretty_print import print_models_short_help

    for info_type, info_map in info.items():
        print(f"\n---{info_type[1]}")
        print_models_short_help(
            *[info_cls for info_cls in info_map.values()],
            show_class_path=show_class_path,
        )


@app.command("list")
def list_commands_and_ops(
    seq: bool = typer.Option(
        True,
        help="Show the available sequence operators.",
    ),
    cmd: bool = typer.Option(
        True,
        help="Show the available piper commands.",
    ),
    details: bool = typer.Option(
        False,
        "--details",
        "-d",
        help="Show a complete field description for each command and operator.",
    ),
):
    """
    List available sequence operators and piper commands.
    """
    if details:
        if cmd:
            _print_details(
                _Helper.get_piper_commands(), show_class_path_and_piper_port=True
            )
        if seq:
            _print_details(
                _Helper.get_sequence_operators(), show_class_path_and_piper_port=False
            )
    else:
        if cmd:
            _print_short_help(_Helper.get_piper_commands(), show_class_path=True)
        if seq:
            _print_short_help(_Helper.get_sequence_operators(), show_class_path=False)


@app.command("info")
def commands_and_ops_info(
    command_or_operator: str = typer.Argument(
        ...,
        help=(
            "A sequence operator or a piper command, ie, a `command-name`, "
            "a `package.module.ClassName` class path or "
            "a `path/to/module.py:ClassName` uri."
        ),
        autocompletion=_Helper.complete_operator(is_cmd=True, is_seq_ops=True),
    )
):
    """
    Get detailed info about a sequence operator or a piper command.
    """
    from pipelime.cli.pretty_print import print_model_info

    info_cls = _Helper.get_operator(command_or_operator)
    if info_cls is None:
        info_cls = _Helper.get_command(command_or_operator)
        show_class_path_and_piper_port = True
    else:
        show_class_path_and_piper_port = False

    if info_cls is None:
        _perr(f"{command_or_operator} is not a sequence operator nor a piper command!")
        _pwarn("Have you added the module with `--module`?")
        raise typer.Exit(1)
    print(f"\n---{info_cls[0][0]}")
    print_model_info(
        info_cls[1],
        show_class_path=show_class_path_and_piper_port,
        show_piper_port=show_class_path_and_piper_port,
    )
