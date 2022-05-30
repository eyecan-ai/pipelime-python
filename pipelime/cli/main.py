import typing as t
from pathlib import Path
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


def _store_opt(last_opt, last__val, all_opts):
    import pydash as py_

    if last_opt is not None:
        curr_val = py_.get(all_opts, last_opt, None)
        if curr_val is None:
            py_.set_(all_opts, last_opt, last__val)
        else:
            if not isinstance(curr_val, tuple):
                curr_val = (curr_val,)
            if not isinstance(last__val, tuple):
                last__val = (last__val,)
            py_.set_(all_opts, last_opt, curr_val + last__val)


def _process_key_arg(arg):
    opt, _, val = arg[1:].partition("=")
    if val:
        val = tuple(_convert_val(v) for v in val.split(","))
        if len(val) == 1:
            val = val[0]
    else:
        val = None
    return opt, val


def _process_val_arg(arg, last_val):
    if last_val is None:
        return _convert_val(arg)
    else:
        if not isinstance(last_val, tuple):
            last_val = (last_val,)
        return last_val + (_convert_val(arg),)


def _extract_options(cmd_args) -> t.Tuple[t.Dict[str, t.Any], t.Dict[str, t.Any]]:
    cfg_last_opt, cfg_last_val, prms_last_opt, prms_last_val = None, None, None, None
    cfg_opts, prms_opts = {}, {}
    expecting_cfg_val: t.Optional[bool] = None

    def _store_last_opt():
        if expecting_cfg_val:
            _store_opt(cfg_last_opt, cfg_last_val, cfg_opts)
        elif expecting_cfg_val is not None:
            _store_opt(prms_last_opt, prms_last_val, prms_opts)

    for extra_arg in cmd_args:
        if extra_arg.startswith("#"):
            _store_last_opt()
            expecting_cfg_val = True
            cfg_last_opt, cfg_last_val = _process_key_arg(extra_arg)
        elif extra_arg.startswith("@"):
            _store_last_opt()
            expecting_cfg_val = False
            prms_last_opt, prms_last_val = _process_key_arg(extra_arg)
        elif expecting_cfg_val:
            cfg_last_val = _process_val_arg(extra_arg, cfg_last_val)
        elif expecting_cfg_val is not None:
            prms_last_val = _process_val_arg(extra_arg, prms_last_val)
    _store_last_opt()
    return cfg_opts, prms_opts


app = typer.Typer()


@app.command(
    add_help_option=False,
    no_args_is_help=True,
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def pl_main(  # noqa: C901
    ctx: typer.Context,
    extra_modules: t.List[str] = typer.Option(
        [], "--module", "-m", help="Additional modules to import."
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output."),
    config: t.Optional[Path] = typer.Option(
        None,
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
        help=(
            "A YAML/JSON file with some or all the options required by the command. "
            "Command line options starting with `#` will update and override "
            "the ones in the file."
        ),
    ),
    params: t.Optional[Path] = typer.Option(
        None,
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
        help=(
            "A YAML/JSON file with some or all the parameters to compile the input "
            "configuration. Command line options starting with `@` will update "
            "and override the ones in the file."
        ),
    ),
    run_all: t.Optional[bool] = typer.Option(
        None,
        help=(
            "In case of multiple configurations, run them all. "
            "Otherwise, run only the first one."
        ),
    ),
    command: str = typer.Argument(
        ...,
        help=(
            "A sequence operator or a piper command, ie, a `command-name`, "
            "a `package.module.ClassName` class path or "
            "a `path/to/module.py:ClassName` uri."
        ),
        autocompletion=PipelimeSymbolsHelper.complete_name(
            is_cmd=True,
            is_seq_ops=False,
            additional_names=[
                ("list", "show commands and operators"),
                ("help", "show help for a command or operator"),
            ],
        ),
    ),
    command_args: t.Optional[t.List[str]] = typer.Argument(
        None,
        help=(
            "Piper command arguments. Options starting with `--` are considered part "
            "of the command configurations, while options starting with `@` are part "
            "of the context, ie, the parameters to compile the input configuration."
        ),
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
        from pipelime.choixe import XConfig
        from pipelime.cli.pretty_print import print_error, print_info

        base_cfg = XConfig() if config is None else XConfig.from_file(config)
        base_prms = XConfig() if params is None else XConfig.from_file(params)

        (cmdline_cfg, cmdline_prms,) = (
            (None, None) if not command_args else _extract_options(command_args)
        )
        cmdline_cfg = XConfig(cmdline_cfg)
        cmdline_prms = XConfig(cmdline_prms)

        base_cfg.deep_update(cmdline_cfg, full_merge=True)
        base_prms.deep_update(cmdline_prms, full_merge=True)
        effective_configs = (
            [base_cfg.process(base_prms)]
            if run_all is not None and not run_all
            else base_cfg.process_all(base_prms)
        )
        for cfg in effective_configs:
            if not cfg.inspect().processed:
                print_error("The configuration has not been fully processed.")
                if verbose:
                    print_info(cfg)
                else:
                    print_info(
                        "Run with --verbose to see the final configuration file."
                    )
                typer.Exit(1)

        if len(effective_configs) > 1 and run_all is None:
            if not typer.confirm(
                f"{len(effective_configs)} configurations found. "
                "Do you want to run them all?"
            ):
                effective_configs = effective_configs[:1]

        cfg_size = len(effective_configs)
        for idx, cfg in enumerate(effective_configs):
            if verbose and cfg_size > 1:
                print_info(f"*** CONFIGURATION {idx}/{cfg_size} ***")
            run_command(command, cfg.to_dict(), verbose)


def run_command(command: str, cmd_args: t.Mapping, verbose: bool):
    """
    Run a piper command.
    """

    from pipelime.cli.pretty_print import (
        print_info,
        print_command_outputs,
    )
    from pipelime.piper import PipelimeCommand
    from pipelime.cli.utils import PipelimeSymbolsHelper

    cmd_cls = PipelimeSymbolsHelper.get_command(command)
    if cmd_cls is None or not issubclass(cmd_cls[1], PipelimeCommand):
        PipelimeSymbolsHelper.show_error_and_help(
            command, should_be_cmd=True, should_be_op=False
        )
        raise typer.Exit(1)
    cmd_cls = cmd_cls[1]

    if verbose:
        print_info(f"\nCreating command `{command}` with options:")
        print_info(cmd_args)

    cmd_obj = cmd_cls(**cmd_args)

    if verbose:
        print_info(f"\nCreated command `{command}`:")
        print_info(repr(cmd_obj))

    if verbose:
        print_info(f"\nRunning `{command}`...")
    cmd_obj()

    print_info(f"\n`{command}` outputs:")
    print_command_outputs(cmd_obj)
