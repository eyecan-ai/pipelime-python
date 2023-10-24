from __future__ import annotations

import functools
import itertools
import typing as t
from pathlib import Path

import typer
from pydantic import BaseModel

from pipelime.cli.parser import CLIParsingError, parse_pipelime_cli
from pipelime.cli.subcommands import SubCommands as subc
from pipelime.cli.utils import (
    PipelimeSymbolsHelper,
    print_command_op_stage_info,
    print_commands_ops_stages_list,
)

if t.TYPE_CHECKING:
    from pipelime.choixe import XConfig
    from pipelime.piper.checkpoint import Checkpoint


class PlCliOptions(BaseModel):
    _namespace: t.ClassVar[str] = "__plmain"

    config: t.List[Path]
    context: t.List[Path]
    keep_tmp: bool
    extra_modules: t.List[str]
    run_all: t.Optional[bool]
    output: t.Optional[Path]
    output_ctx: t.Optional[Path]
    command_outputs: t.Optional[Path]
    verbose: int
    dry_run: bool
    command: str
    command_args: t.List[str]
    pipelime_tmp: t.Optional[str]

    def purged_dict(self):
        return self._purge(self.dict())

    def _purge(self, data):
        if isinstance(data, Path):
            data = data.as_posix()
        elif isinstance(data, bytes):
            data = data.decode()

        if isinstance(data, t.Mapping):
            return {k: self._purge(v) for k, v in data.items()}
        if not isinstance(data, str) and isinstance(data, t.Sequence):
            return [self._purge(v) for v in data]
        return data


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


def _deep_update_fn(to_be_updated: "XConfig", data: "XConfig", verbose: bool):
    from pipelime.cli.pretty_print import print_info

    if verbose:
        print_info("> Merging XConfigs:")
        print_info(">> Base:")
        print_info(to_be_updated.to_dict(), pretty=True)
        print_info(">> To be merged:")
        print_info(data.to_dict(), pretty=True)
    to_be_updated.deep_update(data, full_merge=True)
    if verbose:
        print_info(">> Result:")
        print_info(to_be_updated.to_dict(), pretty=True)
    return to_be_updated


def _process_cfg_or_die(
    cfg: "XConfig",
    ctx: t.Optional["XConfig"],
    cfg_name: str,
    run_all: t.Optional[bool],
    output: t.Optional[Path],
    exit_on_error: bool,
    verbose: bool,
    print_all: bool,
    ask_missing_vars: bool = False,
) -> t.List["XConfig"]:
    from pipelime.choixe.visitors.processor import ChoixeProcessingError
    from pipelime.cli.pretty_print import print_error, print_info, print_warning

    if verbose:
        print_info(f"> Processing {cfg_name}...")

    try:
        effective_configs = (
            [cfg.process(ctx, ask_missing_vars)]
            if run_all is not None and not run_all
            else cfg.process_all(ctx, ask_missing_vars)
        )
    except ChoixeProcessingError as e:
        if exit_on_error:
            print_error(
                f"Invalid {cfg_name}! {e}\nRun with -vv or more to get more info."
            )
            raise typer.Exit(1)
        raise e
    except RecursionError:
        print_error(f"Recursion detected while processing {cfg_name}!")
        print_warning(
            "Please check your context for self-references, eg, `myvar: $var(myvar)`."
        )
        raise typer.Exit(1)

    if verbose:
        pls = "s" if len(effective_configs) != 1 else ""
        print_info(f"> Found {len(effective_configs)} {cfg_name}{pls}")

    if len(effective_configs) > 1 and run_all is None:
        if not typer.confirm(
            f"{len(effective_configs)} {cfg_name}s found. "
            "Do you want to keep them all?"
        ):
            effective_configs = effective_configs[:1]

    if print_all:
        for idx, c in enumerate(effective_configs):
            print_info(f">> {cfg_name} {idx}:")
            print_info(c.to_dict(), pretty=True)

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


def _process_all(
    base_cfg: t.List["XConfig"],
    effective_ctx: "XConfig",
    output: t.Optional[Path],
    run_all: t.Optional[bool],
    exit_on_error: bool,
    verbose: int,
    ask_missing_vars: bool = False,
):
    from pipelime.choixe import XConfig
    from pipelime.cli.pretty_print import print_info

    if verbose > 1:
        print_info("\nProcessing configurations:")

    # first process with no branch
    effective_configs = [
        _process_cfg_or_die(
            c,
            effective_ctx,
            "configuration",
            False,
            output,
            exit_on_error,
            verbose > 1,
            verbose > 3,
            ask_missing_vars,
        )
        for c in base_cfg
        if c.to_dict()
    ]
    if effective_configs:
        # effective_configs = functools.reduce(
        #     lambda acc, curr: acc + curr, effective_configs
        # )
        effective_configs = functools.reduce(
            lambda acc, curr: _deep_update_fn(acc, curr, verbose > 3),
            itertools.chain.from_iterable(effective_configs),
        )
    else:
        effective_configs = XConfig()

    if verbose > 2:
        print_info("\nFinal merged configuration:")
        print_info(effective_configs.to_dict(), pretty=True)

    if verbose > 1:
        print_info("\nProcessing branches:")

    # now process the branches, if any, and check the overall merge
    return _process_cfg_or_die(
        effective_configs,
        effective_ctx,
        "configuration",
        run_all,
        output,
        exit_on_error,
        verbose > 1,
        verbose > 3,
        ask_missing_vars,
    )


class VersionCallback:
    @staticmethod
    def get_version():
        import pipelime

        return pipelime.__version__


def version_callback(value: bool):
    if value:
        print(VersionCallback.get_version())
        raise typer.Exit()


def pl_main(
    ctx: typer.Context,
    config: t.List[Path] = typer.Option(
        None,
        "--config",
        "-c",
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
        help=(
            "One or more yaml/json files with some or "
            "all the arguments required by the command.\n\n"
            "`++opt` or `+opt` command line options update and override them.\n\n "
        ),
        # autocompletion=_complete_yaml,
    ),
    context: t.List[Path] = typer.Option(
        None,
        "--context",
        "-x",
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=False,
        readable=True,
        resolve_path=True,
        help=(
            "One or more yaml/json files with some or all the context parameters. "
            "If `--config` is set and `--context` is not, all files matching "
            "`*context*.[yaml|yml|json]` in the folders of all the "
            "configuration files will be loaded as the context. "
            "Use `--no-ctx-autoload` to disable this behavior.\n\n"
            "`@@opt` or `@opt` command line options update and override them.\n\n"
            "After a `//` token, `++opt` and `+opt` are accepted as well.\n\n "
        ),
        # autocompletion=_complete_yaml,
    ),
    keep_tmp: bool = typer.Option(
        False,
        "--keep-tmp",
        "-t",
        help="DO NOT remove temporary folders, if any, after running this command.",
    ),
    ctx_autoload: bool = typer.Option(
        True,
        help=(
            "Enable/disable context auto-loading when "
            "`--config` is set and `--context` is not."
        ),
    ),
    extra_modules: t.List[str] = typer.Option(
        [],
        "--module",
        "-m",
        help=(
            "Additional modules to import: `class.path.to.module`, "
            "`path/to/module.py` or `<code>`"
        ),
    ),
    run_all: t.Optional[bool] = typer.Option(
        None,
        help=(
            "In case of multiple configurations, run them all or only the first one. "
            "If not specified, user will be notified if multiple configurations "
            "are found and asked on how to proceed."
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
    output_ctx: t.Optional[Path] = typer.Option(
        None,
        writable=True,
        resolve_path=True,
        help="Save final processed context to json/yaml.",
    ),
    command_outputs: t.Optional[Path] = typer.Option(
        None,
        writable=True,
        resolve_path=True,
        help="Save command outputs to json/yaml.",
    ),
    checkpoint: t.Optional[Path] = typer.Option(
        None,
        "--checkpoint",
        "--ckpt",
        "-k",
        writable=True,
        file_okay=False,
        resolve_path=True,
        help="The checkpoint folder. If it already exists, it must be empty.",
    ),
    verbose: int = typer.Option(
        0,
        "--verbose",
        "-v",
        help="Verbose output. Can be specified multiple times.",
        count=True,
    ),
    dry_run: bool = typer.Option(False, "--dry-run", "-d", help="Dry run."),
    command: str = typer.Argument(
        "",
        show_default=False,
        help=(
            (
                "A command, ie, a `command-name`, "
                "a `package.module.ClassName` class path, "
                "a `path/to/module.py:ClassName` uri (use with care) or"
                "a `ClassName:::<code>` for anonymous imports.\n\n"
            )
            + subc.get_help()
        ),
        # autocompletion=PipelimeSymbolsHelper.complete_name(
        #     is_cmd=True,
        #     is_seq_op=False,
        #     is_stage=False,
        #     additional_names=subc.get_autocompletions(),
        # ),
    ),
    command_args: t.Optional[t.List[str]] = typer.Argument(
        None,
        help=(
            "\b\nExpected command line:\n"
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
    {0} Examples:

    `{1} list` prints a list of the available commands, sequence operators
    and stages.

    `{1} help <cmd-op-stg>` prints informations on a specific command, sequence
    operator or stage.

    `{1} <command> [<args>]` runs a{2} command.

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
        try:
            if command and command not in subc.HELP[0]:
                print_command_op_stage_info(
                    command, show_description=verbose > 0, recursive=verbose > 1
                )
            elif command_args:
                print_command_op_stage_info(
                    command_args[0], show_description=verbose > 0, recursive=verbose > 1
                )
            else:
                print(ctx.get_help())
        except ValueError:
            pass
    elif command in subc.WIZARD[0] or any([w in command_args for w in subc.WIZARD[0]]):
        from pipelime.cli.wizard import Wizard

        if command and command not in subc.WIZARD[0]:
            Wizard.model_cfg_wizard(command)
        Wizard.model_cfg_wizard(command_args[0])
    elif command in subc.LIST[0]:
        print_commands_ops_stages_list(
            verbose > 0,
            show_cmds=True,
            show_ops=True,
            show_stages=True,
            show_description=verbose > 0,
            recursive=verbose > 1,
        )
    elif command in subc.LIST_CMDS[0]:
        print_commands_ops_stages_list(
            verbose > 0,
            show_cmds=True,
            show_ops=False,
            show_stages=False,
            show_description=verbose > 0,
            recursive=verbose > 1,
        )
    elif command in subc.LIST_OPS[0]:
        print_commands_ops_stages_list(
            verbose > 0,
            show_cmds=False,
            show_ops=True,
            show_stages=False,
            show_description=verbose > 0,
            recursive=verbose > 1,
        )
    elif command in subc.LIST_STGS[0]:
        print_commands_ops_stages_list(
            verbose > 0,
            show_cmds=False,
            show_ops=False,
            show_stages=True,
            show_description=verbose > 0,
            recursive=verbose > 1,
        )
    elif command:
        from pipelime.choixe.utils.io import PipelimeTmp
        from pipelime.piper.checkpoint import LocalCheckpoint

        # context auto-load
        if config and not context and ctx_autoload:
            context = []
            for c in config:
                for p in c.resolve().parent.glob("*context*.*"):
                    if p.suffix in (".yaml", ".yml", ".json"):
                        context += [p]

        plopts = PlCliOptions(
            config=config or [],
            context=context or [],
            keep_tmp=keep_tmp,
            extra_modules=extra_modules,
            run_all=run_all,
            output=output,
            output_ctx=output_ctx,
            command_outputs=command_outputs,
            verbose=verbose,
            dry_run=dry_run,
            command=command,
            command_args=command_args,
            pipelime_tmp=None,
        )

        if checkpoint:
            # create a new checkpoint
            if checkpoint.exists():
                if not checkpoint.is_dir():
                    raise ValueError(
                        f"Checkpoint folder `{checkpoint}` exists but is not a folder."
                    )
                if any(checkpoint.iterdir()):
                    raise ValueError(f"Checkpoint folder `{checkpoint}` is not empty.")
            plopts.pipelime_tmp = PipelimeTmp.make_session_dir().as_posix()
            ckpt = LocalCheckpoint(folder=checkpoint)
            ckpt.write_data(PlCliOptions._namespace, "", plopts.purged_dict())
        else:
            ckpt = None

        run_with_checkpoint(cli_opts=plopts, checkpoint=ckpt)
    else:
        from pipelime.cli.pretty_print import print_error

        print_error("No command specified.")
        raise typer.Exit(1)


def run_with_checkpoint(cli_opts: PlCliOptions, checkpoint: t.Optional["Checkpoint"]):
    from loguru import logger

    import pipelime.choixe.utils.io as choixe_io
    from pipelime.choixe import XConfig
    from pipelime.cli.pretty_print import print_error, print_info, print_warning

    PipelimeSymbolsHelper.set_extra_modules(cli_opts.extra_modules)

    if cli_opts.pipelime_tmp:
        pltmp = Path(cli_opts.pipelime_tmp)
        if pltmp.is_dir():
            logger.debug(f"Restoring pipelime temp folder to `{pltmp}`")
            choixe_io.PipelimeTmp.SESSION_TMP_DIR = pltmp

    if cli_opts.verbose > 0:

        def _print_file_list(files: t.Sequence[Path], name: str):
            if files:
                flist = ", ".join(f'"{str(c)}"' for c in files)
                print_info(
                    f"{name.capitalize()} file"
                    + (f"s: [ {flist} ]" if len(files) > 1 else f": {files[0]}")
                )
            else:
                print_info(f"No {name} file")

        _print_file_list(cli_opts.config, "configuration")
        _print_file_list(cli_opts.context, "context")
        print_info(
            f"Other command and context arguments: {cli_opts.command_args}"
            if cli_opts.command_args
            else "No other command or context arguments"
        )

    base_cfg = [choixe_io.load(c) for c in cli_opts.config]
    base_ctx = [choixe_io.load(c) for c in cli_opts.context]

    # process extra args
    try:
        cmdline_cfg, cmdline_ctx = parse_pipelime_cli(cli_opts.command_args)
    except CLIParsingError as e:
        e.rich_print()
        raise typer.Exit(1)

    if cli_opts.verbose > 2:
        _print_dict(
            f"Loaded configuration file{'s' if len(cli_opts.config) > 1 else ''}",
            base_cfg,
        )
        _print_dict(
            f"Loaded context file{'s' if len(cli_opts.context) > 1 else ''}", base_ctx
        )
        _print_dict("Configuration options from command line", cmdline_cfg)
        _print_dict("Context options from command line", cmdline_ctx)

    # keep each config separated to get the right cwd
    base_cfg = [
        XConfig(data=c, cwd=p.parent) for c, p in zip(base_cfg, cli_opts.config)
    ]
    base_cfg.append(XConfig(data=cmdline_cfg, cwd=Path.cwd()))

    base_ctx = [
        XConfig(data=c, cwd=p.parent) for c, p in zip(base_ctx, cli_opts.context)
    ]
    base_ctx.append(XConfig(data=cmdline_ctx, cwd=Path.cwd()))

    # process contexts to resolve imports and local loops
    if cli_opts.verbose > 2:
        print_info("\nProcessing context files:")

    def _sum(a: XConfig, b: XConfig) -> XConfig:
        a = XConfig(data=a.to_dict(), cwd=a.get_cwd(), schema=a.get_schema())
        a.deep_update(b, full_merge=True)
        return a

    def _ctx_for_ctx_update(ctx_for_ctx: XConfig, new_ctxs: t.Sequence[XConfig]):
        for newc in new_ctxs:
            ctx_for_ctx = _sum(newc, ctx_for_ctx)
        return ctx_for_ctx

    # use the command line context to process the other contexts
    effective_ctx = []
    ctx_for_ctx = base_ctx[-1]
    for idx, curr_ctx in enumerate(base_ctx):
        if curr_ctx.to_dict():
            # the context itself is a valid context
            curr_ctx_for_ctx = _sum(curr_ctx, ctx_for_ctx)

            if cli_opts.verbose > 3:
                print_info(f"[{idx}] context to process:")
                print_info(curr_ctx.to_dict(), pretty=True)
                print_info(f"[{idx}] context for context")
                print_info(curr_ctx_for_ctx.to_dict(), pretty=True)
            if cli_opts.verbose > 2:
                print_info(f"[{idx}] preprocessing...")

            new_ctxs = _process_cfg_or_die(
                curr_ctx,
                curr_ctx_for_ctx,
                "context",
                cli_opts.run_all,
                None,
                True,
                cli_opts.verbose > 2,
                cli_opts.verbose > 3,
                ask_missing_vars=True,
            )
            partial_ctx_for_ctx = _ctx_for_ctx_update(ctx_for_ctx, new_ctxs)

            if cli_opts.verbose > 3:
                print_info(f"[{idx}] updated context for context")
                print_info(partial_ctx_for_ctx.to_dict(), pretty=True)
            if cli_opts.verbose > 2:
                print_info(f"[{idx}] processing self-references...")

            new_ctxs = _process_cfg_or_die(
                curr_ctx,
                partial_ctx_for_ctx,
                "context",
                cli_opts.run_all,
                None,
                True,
                cli_opts.verbose > 2,
                cli_opts.verbose > 3,
                ask_missing_vars=False,
            )
            ctx_for_ctx = _ctx_for_ctx_update(ctx_for_ctx, new_ctxs)

            if cli_opts.verbose > 3:
                print_info(f"[{idx}] final updated context for context")
                print_info(ctx_for_ctx.to_dict(), pretty=True)

            effective_ctx.extend(new_ctxs)

    if effective_ctx:
        effective_ctx = functools.reduce(
            lambda acc, curr: _deep_update_fn(acc, curr, cli_opts.verbose > 3),
            effective_ctx,
        )
        if cli_opts.output_ctx:
            effective_ctx.save_to(cli_opts.output_ctx)
    else:
        effective_ctx = XConfig()

    if cli_opts.verbose > 1:
        print_info("\nFinal effective context:")
        print_info(effective_ctx.to_dict(), pretty=True)

    if cli_opts.command in subc.AUDIT[0]:
        from dataclasses import fields

        from pipelime.choixe.visitors.processor import ChoixeProcessingError

        print_info("\n📄 CONFIGURATION AUDIT")
        for idx, c in enumerate(base_cfg):
            if len(base_cfg) > 1:
                name = (
                    str(cli_opts.config[idx])
                    if idx < len(cli_opts.config)
                    else "command line"
                )
                print_info(f"\n*** {name}")
            inspect_info = c.inspect()
            for field in fields(inspect_info):
                value = getattr(inspect_info, field.name)
                print_info(f"🔍 {field.name}:")
                if value or isinstance(value, bool):
                    print_info(value, pretty=True, indent_guides=False)

        print_info("\n📄 EFFECTIVE CONTEXT\n")
        print_info(effective_ctx.to_dict(), pretty=True, indent_guides=False)
        print_info("")

        try:
            effective_configs = _process_all(
                base_cfg,
                effective_ctx,
                cli_opts.output,
                cli_opts.run_all,
                False,
                cli_opts.verbose > 2,
                ask_missing_vars=False,  # do not allow missing vars in audit
            )
        except ChoixeProcessingError as e:
            # from rich.prompt import Confirm, Prompt

            # from pipelime.cli.wizard import Wizard

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
        # new_ctx = Wizard.context_wizard(inspect_info.variables, effective_ctx)
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

        cfg_size = len(effective_configs)
        pls = "s" if cfg_size != 1 else ""
        print_info(f"🎉 Configuration successfully processed ({cfg_size} variant{pls}).")

        if cli_opts.verbose > 2:
            print_info("\nFinal effective configurations:")
            for idx, cfg in enumerate(effective_configs):
                print_info(f"\n*** CONFIGURATION {idx+1}/{cfg_size} ***\n")
                print_info(cfg.to_dict(), pretty=True)

        raise typer.Exit(0)
    else:
        from pipelime.cli.pretty_print import show_spinning_status

        input_cfg_keys = set([k for c in base_cfg for k in c.to_dict().keys()])

        with show_spinning_status("Processing configuration and context..."):
            effective_configs = _process_all(
                base_cfg,
                effective_ctx,
                cli_opts.output,
                cli_opts.run_all,
                True,
                cli_opts.verbose,
                ask_missing_vars=True,
            )

        # remove keys not present in the input configs that were inserted
        # when asking the user for missing variables
        for cfg in effective_configs:
            for k in cfg.to_dict().keys():
                if k not in input_cfg_keys:
                    cfg.pop(k)

        cmd_name = cli_opts.command
        cfg_size = len(effective_configs)
        for idx, cfg in enumerate(effective_configs):
            cfg_dict = cfg.to_dict()

            if cli_opts.verbose > 1:
                print_info(f"\n*** CONFIGURATION {idx+1}/{cfg_size} ***\n")
                print_info(cfg_dict, pretty=True)

            if cli_opts.command in subc.EXEC[0]:
                if len(cfg_dict) == 0:
                    print_error("No command specified.")
                    raise typer.Exit(1)
                if len(cfg_dict) > 1:
                    print_error("Multiple commands found.")
                    print_warning("You should use the `run` command to process a dag.")
                    raise typer.Exit(1)
                cmd_name = next(iter(cfg_dict))
                cfg_dict = next(iter(cfg_dict.values()))

            run_command(
                cmd_name,
                cfg_dict,
                cli_opts.verbose,
                cli_opts.dry_run,
                cli_opts.keep_tmp,
                cli_opts.command_outputs,
                checkpoint,
            )


def run_command(
    command: str,
    cmd_args: t.Mapping,
    verbose: int,
    dry_run: bool,
    keep_tmp: bool,
    command_outputs: t.Optional[Path],
    checkpoint: t.Optional["Checkpoint"],
):
    """
    Run a pipelime command.
    """

    import time

    from pydantic.error_wrappers import ValidationError

    from pipelime.choixe.utils.io import PipelimeTmp, dump
    from pipelime.cli.pretty_print import print_command_outputs, print_info
    from pipelime.cli.tui import TuiApp, is_tui_needed
    from pipelime.cli.utils import (
        get_pipelime_command_cls,
        show_field_alias_valerr,
        time_to_str,
    )
    from pipelime.commands import TempCommand

    try:
        cmd_cls = get_pipelime_command_cls(command)
    except ValueError:
        raise typer.Exit(1)

    if is_tui_needed(cmd_cls, cmd_args):
        app = TuiApp(cmd_cls, cmd_args)
        cmd_args = t.cast(t.Mapping, app.run())

    if verbose > 2:
        print_info(f"\nCreating command `{command}` with options:")
        print_info(cmd_args, pretty=True)

    try:
        if checkpoint is None:
            cmd_obj = cmd_cls(**cmd_args)
        else:
            ckpt_ns = checkpoint.get_namespace(command)
            cmd_obj = cmd_cls.init_from_checkpoint(ckpt_ns, **cmd_args)
            cmd_obj._checkpoint = ckpt_ns
    except ValidationError as e:
        show_field_alias_valerr(e)
        raise e

    if verbose > 0:
        print_info(f"\nCreated command `{command}`:")
        print_info(cmd_obj.dict(), pretty=True)

    if dry_run or verbose > 0:
        print_info(f"\nRunning `{command}`...")

    start_time = time.perf_counter_ns()
    if not dry_run:
        cmd_obj()
    end_time = time.perf_counter_ns()
    print_info("\nCommand executed in " + time_to_str(end_time - start_time))

    print_info(f"\n`{command}` outputs:")
    print_command_outputs(cmd_obj)

    if command_outputs:

        def _cvt_data(data):
            if hasattr(data, "__piper_repr__"):
                return data.__piper_repr__()
            if isinstance(data, (bytes, str)):
                return str(data)
            if isinstance(data, Path):
                return data.resolve().absolute().as_posix()
            elif isinstance(data, t.Mapping):
                return {k: _cvt_data(v) for k, v in data.items()}
            elif isinstance(data, t.Sequence):
                return [_cvt_data(v) for v in data]
            return repr(data)

        print_info(f"\nSaving command outputs to `{command_outputs}`")
        outs = {k: _cvt_data(v) for k, v in cmd_obj.get_outputs().items()}
        dump(outs, command_outputs)

    if not keep_tmp and PipelimeTmp.SESSION_TMP_DIR:
        print_info("\nCleaning temporary files...")
        TempCommand(name=PipelimeTmp.SESSION_TMP_DIR.stem, force=True)()  # type: ignore


def _create_typer_app(
    *,
    app_name: str = "Pipelime",
    entry_point: t.Optional[str] = None,
    app_description: t.Optional[str] = None,
    version: t.Optional[str] = None,
    extra_args: t.Sequence[str] = list(),
):
    import sys

    if entry_point is None and len(sys.argv) > 0:
        entry_point = Path(sys.argv[0]).name

    # if there is no other args, run the app with no args
    if extra_args and len(sys.argv) > 1:
        sys.argv.extend(extra_args)

    def _preproc_docs(func):
        desc = (
            app_description
            if app_description
            else f"{app_name} Command Line Interface."
        )
        n_appname = ("n " if app_name[0] in "aeiouAEIOU" else " ") + app_name
        if desc[-1] != ".":
            desc += "."
        func.__doc__ = func.__doc__.format(
            desc, entry_point or app_name.casefold(), n_appname
        )
        return func

    if version is not None:
        VersionCallback.get_version = lambda: version

    app = typer.Typer(pretty_exceptions_enable=False)
    app.command(
        add_help_option=False,
        no_args_is_help=True,
        context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
    )(_preproc_docs(pl_main))
    return app


def run_typer_app(
    *,
    app_name: str = "Pipelime",
    entry_point: t.Optional[str] = None,
    app_description: t.Optional[str] = None,
    version: t.Optional[str] = None,
    extra_args: t.Sequence[str] = list(),
):
    app = _create_typer_app(
        app_name=app_name,
        entry_point=entry_point,
        app_description=app_description,
        version=version,
        extra_args=extra_args,
    )
    app()


if __name__ == "__main__":
    run_typer_app()
