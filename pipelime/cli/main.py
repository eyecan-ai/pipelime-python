import typing as t
import typer

from pipelime.cli.utils import (
    PipelimeSymbolsHelper,
    print_command_or_op_info,
    print_commands_and_ops_list,
)


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


@app.command(
    add_help_option=False,
    no_args_is_help=True,
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def pl_main(
    ctx: typer.Context,
    extra_modules: t.List[str] = typer.Option(
        [], "--module", "-m", help="Additional modules to import."
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output."),
    command: str = typer.Argument(
        ...,
        help=(
            "A sequence operator or a piper command, ie, a `command-name`, "
            "a `package.module.ClassName` class path or "
            "a `path/to/module.py:ClassName` uri."
        ),
    ),
    command_args: t.Optional[t.List[str]] = typer.Argument(
        None, help="Piper command arguments."
    ),
):
    """
    Pipelime Command Line Interface. Examples:

    `pipelime list` prints a list of the available commands and operators.

    `pipelime help <cmd-or-op>` prints informations on a specific command or operator.

    `pipelime <command> [<args>]` runs a piper command.
    """
    PipelimeSymbolsHelper.set_extra_modules(extra_modules)

    if command in ("help", "--help", "-h"):
        if command_args:
            print_command_or_op_info(command_args[0])
        else:
            print(ctx.get_help())
    elif command == "list":
        print_commands_and_ops_list(verbose)
    else:
        run_command(command, [] if command_args is None else command_args, verbose)


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


def run_command(command: str, cmd_args: t.Sequence[str], verbose: bool):
    """
    Run a piper command.
    """

    from pipelime.cli.pretty_print import (
        print_error,
        print_info,
        print_warning,
        print_command_outputs,
    )
    from pipelime.piper import PipelimeCommand

    cmd_cls = PipelimeSymbolsHelper.get_command(command)
    if cmd_cls is None or not issubclass(cmd_cls[1], PipelimeCommand):
        print_error(f"{command} is not a piper command!")
        print_warning("Have you added the module with `--module`?")
        raise typer.Exit(1)
    cmd_cls = cmd_cls[1]

    last_opt = None
    last_val = None
    all_opts = {}

    for extra_arg in cmd_args:
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
        print_info(f"\nCreating command `{command}` with options:")
        print_info(all_opts)

    cmd_obj = cmd_cls(**all_opts)

    if verbose:
        print_info(f"\nCreated command `{command}`:")
        print_info(repr(cmd_obj))

    if verbose:
        print_info(f"\nRunning `{command}`...")
    cmd_obj()

    print_info(f"\n`{command}` outputs:")
    print_command_outputs(cmd_obj)
