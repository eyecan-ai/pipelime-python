import typing as t
from pathlib import Path
import typer

from pipelime.cli.utils import (
    PipelimeSymbolsHelper,
    print_command_or_op_info,
    print_commands_and_ops_list,
)


def _complete_yaml(incomplete: str):
    for v in Path(".").glob(f"{incomplete}*.yaml"):
        yield str(v)


def _print_xconfig(name, data):
    import json
    from pipelime.cli.pretty_print import print_info

    print_info(f"\n{name}:")
    print_info(json.dumps(data.to_dict(), indent=2))


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
    from pipelime.choixe.utils.common import deep_set_

    cfg_last_opt, cfg_last_val, prms_last_opt, prms_last_val = None, None, None, None
    cfg_opts, prms_opts = {}, {}
    expecting_cfg_val: t.Optional[bool] = None

    def _store_last_opt():
        if expecting_cfg_val:
            deep_set_(
                cfg_opts,
                key_path=cfg_last_opt,  # type: ignore
                value=cfg_last_val if cfg_last_val is not None else True,
                append=True,
            )
        elif expecting_cfg_val is not None:
            deep_set_(
                prms_opts,
                key_path=prms_last_opt,  # type: ignore
                value=prms_last_val if prms_last_val is not None else True,
                append=True,
            )

    for extra_arg in cmd_args:
        if extra_arg.startswith("+"):
            _store_last_opt()
            expecting_cfg_val = True
            cfg_last_opt, cfg_last_val = _process_key_arg(extra_arg)
        elif extra_arg.startswith("!"):
            _store_last_opt()
            expecting_cfg_val = False
            prms_last_opt, prms_last_val = _process_key_arg(extra_arg)
        elif expecting_cfg_val:
            cfg_last_val = _process_val_arg(extra_arg, cfg_last_val)
        elif expecting_cfg_val is not None:
            prms_last_val = _process_val_arg(extra_arg, prms_last_val)
    _store_last_opt()
    return cfg_opts, prms_opts


def _process_cfg_or_die(cfg, ctx, run_all: t.Optional[bool], output: t.Optional[Path]):
    from pipelime.cli.pretty_print import print_error
    from pipelime.choixe.visitors.processor import ChoixeProcessingError

    try:
        effective_configs = (
            [cfg.process(ctx)]
            if run_all is not None and not run_all
            else cfg.process_all(ctx)
        )
    except ChoixeProcessingError as e:
        print_error(f"Invalid configuration! {e}")
        raise typer.Exit(1)

    if output is not None:
        zero_fill = len(str(len(effective_configs) - 1))
        for idx, cfg in enumerate(effective_configs):
            filepath = (
                output.with_name(f"{output.name}_{str(idx).zfill(zero_fill)}")
                if len(effective_configs) > 1
                else output
            )
            cfg.save_to(filepath)
    return effective_configs


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
    dry_run: bool = typer.Option(False, "--dry-run", "-d", help="Dry run."),
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
            "Command line options starting with `+` will update and override "
            "the ones in the file."
        ),
        autocompletion=_complete_yaml,
    ),
    context: t.Optional[Path] = typer.Option(
        None,
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
        help=(
            "A YAML/JSON file with some or all the context parameters to compile the "
            "input configuration. Command line options starting with `!` will update "
            "and override the ones in the file."
        ),
        autocompletion=_complete_yaml,
    ),
    run_all: t.Optional[bool] = typer.Option(
        None,
        help=(
            "In case of multiple configurations, run them all, "
            "otherwise, run only the first one. If not specified, user will be "
            "notified if multiple configurations are found."
        ),
    ),
    output: t.Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        writable=True,
        resolve_path=True,
        help="Save final processed configuration to JSON/YAML.",
    ),
    command: str = typer.Argument(
        "",
        show_default=False,
        help=(
            "A sequence operator or a pipelime command, ie, a `command-name`, "
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
            "Pipelime command arguments. Options starting with `+` are considered part "
            "of the command configurations, while options starting with `!` are part "
            "of the context, ie, the parameters to compile the input configuration."
        ),
    ),
    help: bool = typer.Option(
        False, "--help", "-h", show_default=False, help="Show this message and exit."
    ),
):
    """
    Pipelime Command Line Interface. Examples:

    `pipelime list` prints a list of the available commands and sequence operators.

    `pipelime help <cmd-or-op>` prints informations on a specific command or sequence
    operator.

    `pipelime <command> [<args>]` runs a pipelime command.
    """
    PipelimeSymbolsHelper.set_extra_modules(extra_modules)

    if command_args is None:
        command_args = []

    if help or command == "help" or "help" in command_args:
        if command and command != "help":
            print_command_or_op_info(command)
        elif command_args:
            print_command_or_op_info(command_args[0])
        else:
            print(ctx.get_help())
    elif command == "list":
        print_commands_and_ops_list(verbose)
    elif command:
        from pipelime.choixe import XConfig
        from pipelime.cli.pretty_print import print_error, print_info

        base_cfg = XConfig() if config is None else XConfig.from_file(config)
        base_ctx = XConfig() if context is None else XConfig.from_file(context)

        cmdline_cfg, cmdline_ctx = _extract_options(command_args)
        cmdline_cfg = XConfig(cmdline_cfg)
        cmdline_ctx = XConfig(cmdline_ctx)

        if verbose:
            _print_xconfig("Loaded configuration file", base_cfg)
            _print_xconfig("Loaded context file", base_ctx)
            _print_xconfig("Configuration options from command line", cmdline_cfg)
            _print_xconfig("Context options from command line", cmdline_ctx)

        base_cfg.deep_update(cmdline_cfg, full_merge=True)
        base_ctx.deep_update(cmdline_ctx, full_merge=True)

        if verbose:
            _print_xconfig("Merged configuration", base_cfg)
            _print_xconfig("Merged context", base_ctx)

        if command == "audit":
            from dataclasses import fields

            print_info("\n\U0001F4C4 CONFIGURATION AUDIT\n")
            data = base_cfg.inspect()
            for field in fields(data):
                value = getattr(data, field.name)
                print_info(f"\U0001F50D {field.name}:")
                if value or isinstance(value, bool):
                    print_info(value, pretty=True)

            print_info("\n\U0001F4C4 CONTEXT AUDIT\n")
            print_info(base_ctx.to_dict(), pretty=True)

            _process_cfg_or_die(base_cfg, base_ctx, run_all, output)
            raise typer.Exit(0)
        else:
            effective_configs = _process_cfg_or_die(base_cfg, base_ctx, run_all, output)

            if verbose:
                pls = "s" if not effective_configs or len(effective_configs) > 1 else ""
                print_info(f"Found {len(effective_configs)} configuration{pls}")

            for cfg in effective_configs:
                if not cfg.inspect().processed:
                    print_error("The configuration has not been fully processed.")
                    if verbose:
                        _print_xconfig("Unprocessed data", cfg)
                    else:
                        print_info(
                            "Run with --verbose to see the final configuration file."
                        )
                    raise typer.Exit(1)

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
                run_command(command, cfg.to_dict(), verbose, dry_run)
    else:
        from pipelime.cli.pretty_print import print_error

        print_error("No command specified.")
        raise typer.Exit(1)


def run_command(command: str, cmd_args: t.Mapping, verbose: bool, dry_run: bool):
    """
    Run a pipelime command.
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
        print_info(cmd_args, pretty=True)

    cmd_obj = cmd_cls(**cmd_args)

    if verbose:
        print_info(f"\nCreated command `{command}`:")
        print_info(cmd_obj.dict(), pretty=True)

    if verbose:
        print_info(f"\nRunning `{command}`...")

    if not dry_run:
        cmd_obj()

    print_info(f"\n`{command}` outputs:")
    print_command_outputs(cmd_obj)
