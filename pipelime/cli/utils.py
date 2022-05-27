import typing as t
from types import ModuleType


class PipelimeSymbolsHelper:
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
            from pipelime.cli.pretty_print import print_error, print_warning

            print_error(f"Found duplicate command `{cmd_name}`")
            print_warning(f"Defined as `{first.classpath()}`")
            print_warning(f"       and `{second.classpath()}`")
            raise ValueError(f"Duplicate command `{cmd_name}`")

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
                    "Sequence Piped Operation",
                    "Sequence Piped Operations",
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
        for op_type, op_dict in PipelimeSymbolsHelper.get_sequence_operators().items():
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

        for cmd_type, cmd_dict in PipelimeSymbolsHelper.get_piper_commands().items():
            if command_name in cmd_dict:
                return (cmd_type, cmd_dict[command_name])
        return None


def print_command_or_op_info(command_or_operator: str):
    """
    Prints detailed info about a sequence operator or a piper command.
    """
    from pipelime.cli.pretty_print import (
        print_error,
        print_info,
        print_model_info,
        print_warning,
    )

    info_cls = PipelimeSymbolsHelper.get_operator(command_or_operator)
    if info_cls is None:
        info_cls = PipelimeSymbolsHelper.get_command(command_or_operator)
        show_class_path_and_piper_port = True
    else:
        show_class_path_and_piper_port = False

    if info_cls is None:
        print_error(
            f"{command_or_operator} is not a sequence operator nor a piper command!"
        )
        print_warning("Have you added the module with `--module`?")
        raise ValueError(
            f"{command_or_operator} is not a sequence operator nor a piper command!"
        )
    print_info(f"\n---{info_cls[0][0]}")
    print_model_info(
        info_cls[1],
        show_class_path=show_class_path_and_piper_port,
        show_piper_port=show_class_path_and_piper_port,
    )


def _print_details(info, show_class_path_and_piper_port):
    from pipelime.cli.pretty_print import print_info, print_model_info

    for info_type, info_map in info.items():
        for info_cls in info_map.values():
            print_info(f"\n---{info_type[0]}")
            print_model_info(
                info_cls,
                show_class_path=show_class_path_and_piper_port,
                show_piper_port=show_class_path_and_piper_port,
            )


def _print_short_help(info, show_class_path):
    from pipelime.cli.pretty_print import print_info, print_models_short_help

    for info_type, info_map in info.items():
        print_info(f"\n---{info_type[1]}")
        print_models_short_help(
            *[info_cls for info_cls in info_map.values()],
            show_class_path=show_class_path,
        )


def print_commands_and_ops_list(show_details: bool = False):
    """
    Print a list of all available sequence operators and piper commands.
    """
    print_fn = _print_details if show_details else _print_short_help
    print_fn(PipelimeSymbolsHelper.get_piper_commands(), True)
    print_fn(PipelimeSymbolsHelper.get_sequence_operators(), False)
