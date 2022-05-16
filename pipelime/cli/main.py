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
    cached_cmds: t.Optional[t.Dict[t.Tuple[str, str], t.Dict]] = None
    cached_seq_ops: t.Optional[t.Dict[t.Tuple[str, str], t.Dict]] = None

    @classmethod
    def set_extra_modules(cls, modules: t.Sequence[str]):
        cls.extra_modules = list(modules)

    @classmethod
    def is_cache_valid(cls) -> bool:
        return all(
            m in cls.cached_modules for m in (cls.std_cmd_modules + cls.extra_modules)
        )

    @classmethod
    def import_modules(cls):
        import pipelime.choixe.utils.imports as pl_imports

        if not cls.is_cache_valid():
            for module_name in cls.std_cmd_modules + cls.extra_modules:
                if module_name not in cls.cached_modules:
                    cls.cached_modules[module_name] = (
                        pl_imports.import_module_from_file(module_name)
                        if module_name.endswith(".py")
                        else pl_imports.import_module_from_path(module_name)
                    )

    @classmethod
    def get_sequence_operators(cls):
        import pipelime.sequences as pls

        if cls.cached_seq_ops is None or not cls.is_cache_valid():
            cls.import_modules()
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

        return cls.cached_seq_ops

    @classmethod
    def get_piper_commands(cls):
        import inspect

        from pipelime.piper import PipelimeCommand

        if cls.cached_cmds is None or not cls.is_cache_valid():
            cls.import_modules()
            all_cmds = {}
            for module_name, module_ in cls.cached_modules.items():
                module_cmds = {
                    cmd_cls.command_title(): cmd_cls
                    for _, cmd_cls in inspect.getmembers(
                        module_,
                        lambda v: inspect.isclass(v)
                        and issubclass(v, PipelimeCommand)
                        and v is not PipelimeCommand,
                    )
                }

                if module_name.endswith(".py"):
                    for cmd_cls in module_cmds.values():
                        cmd_cls._classpath = f"{module_name}:{cmd_cls.__name__}"

                all_cmds = {**all_cmds, **module_cmds}
            cls.cached_cmds = {("Piper Command", "Piper Commands"): all_cmds}
        return cls.cached_cmds

    @classmethod
    def get_operator(cls, operator_name: str):
        for op_type, op_dict in _Helper.get_sequence_operators().items():
            if operator_name in op_dict:
                return (op_type, op_dict[operator_name])
        return None

    @classmethod
    def get_command(cls, command_name: str):
        import pipelime.choixe.utils.imports as pl_imports

        if "." in command_name or ":" in command_name:
            try:
                return (
                    ("Imported Command", "Imported Commands"),
                    pl_imports.import_symbol(command_name),
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
        val = last_val if last_val is not None else True

        curr_val = py_.get(all_opts, last_opt, None)
        if curr_val is None:
            py_.set_(all_opts, last_opt, val)
        else:
            if not isinstance(curr_val, list):
                curr_val = [curr_val]
            py_.set_(all_opts, last_opt, [val] + curr_val)


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

    cmd_cls = _Helper.get_command(command)
    if cmd_cls is None:
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
        _pinfo(cmd_obj.dict())

    if verbose:
        _pinfo("\nRunning `{command}`...")
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
