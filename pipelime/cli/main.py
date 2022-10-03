from __future__ import annotations
import typing as t
from pathlib import Path
from abc import ABC, abstractmethod
import typer

from pipelime.cli.subcommands import SubCommands as subc

from pipelime.cli.utils import (
    PipelimeSymbolsHelper,
    print_command_op_stage_info,
    print_commands_ops_stages_list,
)


class CLISpecialChars:
    @staticmethod
    def assignment():
        return ("=",)

    @staticmethod
    def config():
        # MUST BE SORTED FROM THE LONGEST TO THE SHORTEST
        return ("++", "+")

    @staticmethod
    def context():
        # MUST BE SORTED FROM THE LONGEST TO THE SHORTEST
        return ("@@", "@")

    @staticmethod
    def ctx_start():
        return ("//",)


class CLIParserState(ABC):
    def __init__(
        self,
        cfg_opts: t.Dict[str, t.Any] = {},
        ctx_opts: t.Dict[str, t.Any] = {},
        ctx_started: bool = False,
    ):
        self.cfg_opts = cfg_opts
        self.ctx_opts = ctx_opts
        self.ctx_started = ctx_started

    @abstractmethod
    def process_token(self, token: str) -> CLIParserState:
        pass

    @abstractmethod
    def close(self):
        pass


class CLIParserHoldState(CLIParserState):
    def process_token(self, token: str) -> CLIParserState:
        from pipelime.cli.pretty_print import print_error

        if token in CLISpecialChars.ctx_start():
            return CLIParserHoldState(self.cfg_opts, self.ctx_opts, True)
        if not self.ctx_started and token.startswith(CLISpecialChars.config()):
            opt, val = self._process_key_arg(token)
            cli_state = CLIParserExpectingCfgValue(
                opt, self.cfg_opts, self.ctx_opts, self.ctx_started
            )
            if val is not None:
                return cli_state.process_token(val)
            return cli_state
        if (
            self.ctx_started and token.startswith(CLISpecialChars.config())
        ) or token.startswith(CLISpecialChars.context()):
            opt, val = self._process_key_arg(token)
            cli_state = CLIParserExpectingCtxValue(
                opt, self.cfg_opts, self.ctx_opts, self.ctx_started
            )
            if val is not None:
                return cli_state.process_token(val)
            return cli_state

        print_error(f"Unexpected token: `{token}`")
        raise typer.Exit(1)

    def close(self):
        return

    def _process_key_arg(self, token: str):
        from pipelime.cli.pretty_print import print_error, print_warning

        opt, val = token, None
        for char in CLISpecialChars.config() + CLISpecialChars.context():
            if token.startswith(char):
                opt = token[len(char) :]  # noqa: E203
                break

        for char in CLISpecialChars.assignment():
            if char in opt:
                opt, _, val = opt.partition(char)
                break

        if opt.endswith(".") or ".." in opt or ".[" in opt:
            print_error(f"Invalid key path: `{opt}`")
            print_warning(
                "Remember: Bash and other shells want the choixe's dollar sign (`$`) "
                "escaped! Try with single quotes or backslash."
            )
            print_warning("For example:")
            print_warning(
                "bash/zsh: +operations.map.$model => '+operations.map.$model'"
            )
            print_warning(r"zsh: +operations.map.$model => +operations.map.\$model")
            raise typer.Exit(1)

        return opt, val


class CLIParserExpectingValue(CLIParserState):
    def __init__(
        self,
        key_name: str,
        cfg_opts: t.Dict[str, t.Any] = {},
        ctx_opts: t.Dict[str, t.Any] = {},
        ctx_started: bool = False,
    ):
        super().__init__(cfg_opts, ctx_opts, ctx_started)
        self.key_name = key_name

    @abstractmethod
    def target_cfg(self) -> t.Dict[str, t.Any]:
        pass

    def process_token(self, token: str) -> CLIParserState:
        from pipelime.choixe.utils.common import deep_set_

        if token in CLISpecialChars.ctx_start() or token.startswith(
            CLISpecialChars.config() + CLISpecialChars.context()
        ):
            self._set_boolean_flag()
            cli_state = CLIParserHoldState(
                self.cfg_opts, self.ctx_opts, self.ctx_started
            )
            return cli_state.process_token(token)

        deep_set_(
            self.target_cfg(),
            key_path=self.key_name,
            value=self._convert_val(token),
            append=True,
        )
        return CLIParserHoldState(self.cfg_opts, self.ctx_opts, self.ctx_started)

    def close(self):
        self._set_boolean_flag()

    def _set_boolean_flag(self):
        from pipelime.choixe.utils.common import deep_set_

        deep_set_(
            self.target_cfg(),
            key_path=self.key_name,
            value=True,
            append=True,
        )

    def _convert_val(self, val: str):
        if val.lower() == "true":
            return True
        if val.lower() == "false":
            return False
        if val.lower() in ("none", "null", "nul"):
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


class CLIParserExpectingCfgValue(CLIParserExpectingValue):
    def target_cfg(self) -> t.Dict[str, t.Any]:
        return self.cfg_opts


class CLIParserExpectingCtxValue(CLIParserExpectingValue):
    def target_cfg(self) -> t.Dict[str, t.Any]:
        return self.ctx_opts


def _complete_yaml(incomplete: str):
    for v in Path(".").glob(f"{incomplete}*.yaml"):
        yield str(v)


def _print_dict(name, data):
    import json
    from pipelime.cli.pretty_print import print_info

    print_info(f"\n{name}:")
    print_info(json.dumps(data, indent=2))


def _update_dispatch(original_value, item):
    if isinstance(item, t.Mapping):
        if isinstance(original_value, t.MutableMapping):
            _dict_update(original_value, item)
            return True
    elif isinstance(item, t.Sequence):
        if isinstance(original_value, t.MutableSequence):
            _list_update(original_value, item)
            return True
    return False


def _list_update(to_be_updated: t.MutableSequence, data: t.Sequence):
    to_be_updated.extend([None] * (len(data) - len(to_be_updated)))  # this is safe
    if data[-1] is None:
        del to_be_updated[len(data) - 1 :]  # noqa: E203
    for idx, item in enumerate(data):
        if item is not None:
            original_value = to_be_updated[idx]
            if _update_dispatch(original_value, item):
                continue
            to_be_updated[idx] = item


def _dict_update(to_be_updated: t.MutableMapping, data: t.Mapping):
    for k, v in data.items():
        if k in to_be_updated:
            original_value = to_be_updated[k]
            if _update_dispatch(original_value, v):
                continue
        to_be_updated[k] = v


def _process_cfg_or_die(
    cfg,
    ctx,
    run_all: t.Optional[bool],
    output: t.Optional[Path],
    exit_on_error: bool = True,
):
    from pipelime.cli.pretty_print import print_error
    from pipelime.choixe.visitors.processor import ChoixeProcessingError

    try:
        effective_configs = (
            [cfg.process(ctx)]
            if run_all is not None and not run_all
            else cfg.process_all(ctx)
        )
    except ChoixeProcessingError as e:
        if exit_on_error:
            print_error(f"Invalid configuration! {e}")
            raise typer.Exit(1)
        raise e

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


app = typer.Typer(pretty_exceptions_enable=False)


def version_callback(value: bool):
    from pipelime import __version__

    if value:
        print(__version__)
        raise typer.Exit()


@app.command(
    add_help_option=False,
    no_args_is_help=True,
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def pl_main(  # noqa: C901
    ctx: typer.Context,
    config: t.Optional[Path] = typer.Option(
        None,
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
        help=(
            "A yaml/json file with some or all the arguments "
            "required by the command.\n\n"
            "`++opt` or `+opt` command line options update and override them.\n\n "
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
            "A yaml/json file with some or all the context parameters.\n\n"
            "`@@opt` or `@opt` command line options update and override them.\n\n"
            "After a `//` token, `++opt` and `+opt` are accepted as well.\n\n "
        ),
        autocompletion=_complete_yaml,
    ),
    extra_modules: t.List[str] = typer.Option(
        [], "--module", "-m", help="Additional modules to import."
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
        help="Save final processed configuration to json/yaml.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output."),
    dry_run: bool = typer.Option(False, "--dry-run", "-d", help="Dry run."),
    command: str = typer.Argument(
        "",
        show_default=False,
        help=(
            (
                "A pipelime command, ie, a `command-name`, "
                "a `package.module.ClassName` class path or "
                "a `path/to/module.py:ClassName` uri (use with care).\n\n"
            )
            + subc.get_help()
        ),
        autocompletion=PipelimeSymbolsHelper.complete_name(
            is_cmd=True,
            is_seq_op=False,
            is_stage=False,
            additional_names=subc.get_autocompletions(),
        ),
    ),
    command_args: t.Optional[t.List[str]] = typer.Argument(
        None,
        help=(
            "\b\nPipelime command arguments:\n"
            "- `++opt` and `+opt` are command parameters\n"
            "- `@@opt` and `@opt` are context parameters\n"
            "- after `//` `++opt`, `+opt`, `@@opt`and `@opt` "
            "are always context parameters."
        ),
    ),
    help: bool = typer.Option(
        False, "--help", "-h", show_default=False, help="Show this message and exit."
    ),
    version: t.Optional[bool] = typer.Option(
        None,
        "--version",
        show_default=False,
        help="Show version and exit.",
        callback=version_callback,
        is_eager=True,
    ),
):
    """
    Pipelime Command Line Interface. Examples:

    `pipelime list` prints a list of the available commands, sequence operators
    and stages.

    `pipelime help <cmd-op-stg>` prints informations on a specific command, sequence
    operator or stage.

    `pipelime <command> [<args>]` runs a pipelime command.

    NB: command (++opt) and context (@@opt) arguments with no value are treated as
    TRUE boolean values. Use `false` or `true` to explicitly set a boolean
    and `none`/`null`/`nul` to enforce `None`.
    """

    PipelimeSymbolsHelper.set_extra_modules(extra_modules)

    if command_args is None:
        command_args = []

    if (
        help
        or command in subc.HELP[0]
        or any([h in command_args for h in subc.HELP[0]])
    ):
        if command and command not in subc.HELP[0]:
            print_command_op_stage_info(command)
        elif command_args:
            print_command_op_stage_info(command_args[0])
        else:
            print(ctx.get_help())
    elif command in subc.WIZARD[0] or any([w in command_args for w in subc.WIZARD[0]]):
        from pipelime.cli.wizard import Wizard

        if command and command not in subc.WIZARD[0]:
            Wizard.model_cfg_wizard(command)
        Wizard.model_cfg_wizard(command_args[0])
    elif command in subc.LIST[0]:
        print_commands_ops_stages_list(
            verbose, show_cmds=True, show_ops=True, show_stages=True
        )
    elif command in subc.LIST_CMDS[0]:
        print_commands_ops_stages_list(
            verbose, show_cmds=True, show_ops=False, show_stages=False
        )
    elif command in subc.LIST_OPS[0]:
        print_commands_ops_stages_list(
            verbose, show_cmds=False, show_ops=True, show_stages=False
        )
    elif command in subc.LIST_STGS[0]:
        print_commands_ops_stages_list(
            verbose, show_cmds=False, show_ops=False, show_stages=True
        )
    elif command:
        from pipelime.choixe import XConfig
        import pipelime.choixe.utils.io as choixe_io
        from pipelime.cli.pretty_print import print_error, print_warning, print_info

        if verbose:
            print_info(
                "No configuration file"
                if config is None
                else f"Configuration file: {config}"
            )
            print_info(
                "No context file" if context is None else f"Context file: {config}"
            )
            print_info(
                f"Other command and context arguments: {command_args}"
                if command_args
                else "No other command or context arguments"
            )

        base_cfg = {} if config is None else choixe_io.load(config)
        base_ctx = {} if context is None else choixe_io.load(context)

        # process extra args
        cli_state = CLIParserHoldState()
        for token in command_args:
            cli_state = cli_state.process_token(token)
        cli_state.close()
        cmdline_cfg, cmdline_ctx = cli_state.cfg_opts, cli_state.ctx_opts

        if verbose:
            _print_dict("Loaded configuration file", base_cfg)
            _print_dict("Loaded context file", base_ctx)
            _print_dict("Configuration options from command line", cmdline_cfg)
            _print_dict("Context options from command line", cmdline_ctx)

        _dict_update(base_cfg, cmdline_cfg)
        _dict_update(base_ctx, cmdline_ctx)

        if verbose:
            _print_dict("Merged configuration", base_cfg)
            _print_dict("Merged context", base_ctx)

        base_cfg = XConfig(
            data=base_cfg, cwd=Path.cwd() if config is None else config.parent
        )
        base_ctx = XConfig(
            data=base_ctx, cwd=Path.cwd() if context is None else context.parent
        )

        if command in subc.AUDIT[0]:
            from dataclasses import fields
            from pipelime.choixe.visitors.processor import ChoixeProcessingError

            print_info("\n📄 CONFIGURATION AUDIT\n")
            inspect_info = base_cfg.inspect()
            for field in fields(inspect_info):
                value = getattr(inspect_info, field.name)
                print_info(f"🔍 {field.name}:")
                if value or isinstance(value, bool):
                    print_info(value, pretty=True)

            print_info("\n📄 CONTEXT AUDIT\n")
            print_info(base_ctx.to_dict(), pretty=True)
            print_info("")

            try:
                effective_configs = _process_cfg_or_die(
                    base_cfg, base_ctx, run_all, output, False
                )
            except ChoixeProcessingError as e:
                from rich.prompt import Prompt, Confirm
                from pipelime.cli.wizard import Wizard

                print_warning("Some variables are not defined in the context.")
                print_error(f"Invalid configuration! {e}")
                raise typer.Exit(1)

                ### SKIP FOR NOW
                # if not Confirm.ask(
                #     "Do you want to create a new context?", default=True
                # ):
                #     print_error(f"Invalid configuration! {e}")
                #     raise typer.Exit(1)
            #
            # print_info("\n📝 Please enter a value for each variable")
            # new_ctx = Wizard.context_wizard(inspect_info.variables, base_ctx)
            #
            # print_info("Processing configuration and context...", end="")
            # effective_configs = _process_cfg_or_die(
            #     base_cfg, new_ctx, run_all, output
            # )
            # print_info(" OK")
            #
            # outfile = Prompt.ask("\n💾 Write to (leave empty to skip)")
            # if outfile:
            #     new_ctx.save_to(Path(outfile).with_suffix(".yaml"))

            pls = "s" if len(effective_configs) != 1 else ""
            print_info(
                "🎉 Configuration successfully processed "
                f"({len(effective_configs)} variant{pls})."
            )
            raise typer.Exit(0)
        else:
            effective_configs = _process_cfg_or_die(base_cfg, base_ctx, run_all, output)

            if verbose:
                pls = "s" if len(effective_configs) != 1 else ""
                print_info(f"Found {len(effective_configs)} configuration{pls}")

            for cfg in effective_configs:
                if not cfg.inspect().processed:
                    print_error("The configuration has not been fully processed.")
                    if verbose:
                        _print_dict("Unprocessed data", cfg)
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
                cfg_dict = cfg.to_dict()
                cmd_name = command

                if cmd_name in subc.EXEC[0]:
                    if len(cfg_dict) == 0:
                        print_error("No command specified.")
                        raise typer.Exit(1)
                    if len(cfg_dict) > 1:
                        print_error("Multiple commands found.")
                        print_warning(
                            "You should use the `run` command to process a dag."
                        )
                        raise typer.Exit(1)
                    cmd_name = next(iter(cfg_dict))
                    cfg_dict = next(iter(cfg_dict.values()))

                run_command(cmd_name, cfg_dict, verbose, dry_run)
    else:
        from pipelime.cli.pretty_print import print_error

        print_error("No command specified.")
        raise typer.Exit(1)


def run_command(command: str, cmd_args: t.Mapping, verbose: bool, dry_run: bool):
    """
    Run a pipelime command.
    """

    import time

    from pipelime.cli.pretty_print import (
        print_info,
        print_command_outputs,
    )
    from pipelime.piper import PipelimeCommand
    from pipelime.cli.utils import PipelimeSymbolsHelper, time_to_str

    cmd_cls = PipelimeSymbolsHelper.get_command(command)
    if cmd_cls is None or not issubclass(cmd_cls[1], PipelimeCommand):
        PipelimeSymbolsHelper.show_error_and_help(
            command, should_be_cmd=True, should_be_op=False, should_be_stage=False
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

    start_time = time.perf_counter_ns()
    if not dry_run:
        cmd_obj()
    end_time = time.perf_counter_ns()
    print_info("\nCommand executed in " + time_to_str(end_time - start_time))

    print_info(f"\n`{command}` outputs:")
    print_command_outputs(cmd_obj)


if __name__ == "__main__":
    app()
