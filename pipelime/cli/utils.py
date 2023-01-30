import typing as t
from types import ModuleType
from pydantic import BaseModel

if t.TYPE_CHECKING:
    from pipelime.piper.model import PipelimeCommand


class ActionInfo(BaseModel):
    action: t.Callable
    name: str
    description: str
    classpath: str


class PipelimeSymbolsHelper:
    std_modules = ["pipelime.commands", "pipelime.stages"]
    extra_modules: t.List[str] = []

    cached_modules: t.Dict[str, ModuleType] = {}
    cached_cmds: t.Dict[t.Tuple[str, str], t.Dict] = {}
    cached_seq_ops: t.Dict[t.Tuple[str, str], t.Dict] = {}
    cached_stages: t.Dict[t.Tuple[str, str], t.Dict] = {}

    registered_actions: t.Dict[str, ActionInfo] = {}

    @classmethod
    def complete_name(
        cls,
        is_cmd: bool,
        is_seq_op: bool,
        is_stage: bool,
        *,
        additional_names: t.Sequence[t.Tuple[str, str]] = [],
    ):
        import inspect

        valid_completion_items = (
            list((cls.get_pipelime_commands() if is_cmd else {}).values())
            + list((cls.get_sequence_operators() if is_seq_op else {}).values())
            + list((cls.get_sample_stages() if is_stage else {}).values())
        )
        valid_completion_items = [
            (k, inspect.getdoc(v) or "")
            for elem in valid_completion_items
            for k, v in elem.items()
        ] + list(additional_names)

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
            m in cls.cached_modules for m in (cls.std_modules + cls.extra_modules)
        )

    @classmethod
    def register_action(cls, name: str, info: ActionInfo):
        if name in cls.registered_actions:
            raise ValueError(f"Action `{name}` already registered")
        cls.registered_actions[name] = info

    @classmethod
    def _symbol_name(cls, symbol):
        from pydantic import BaseModel

        return (
            symbol.__config__.title
            if issubclass(symbol, BaseModel) and symbol.__config__.title
            else symbol.__name__
        )

    @classmethod
    def _warn_double_def(cls, type_, name, first, second):
        from pipelime.cli.pretty_print import (
            print_error,
            print_warning,
            get_model_classpath,
        )

        print_error(f"Found duplicate {type_} `{name}`")
        print_warning(f"Defined as `{get_model_classpath(first)}`")
        print_warning(f"       and `{get_model_classpath(second)}`")
        raise ValueError(f"Duplicate {type_} `{name}`")

    @classmethod
    def _load_symbols(cls, base_cls: t.Type, symbol_type: str):
        import inspect

        all_syms = {}
        for module_name, module_ in cls.cached_modules.items():
            module_symbols = tuple(
                (cls._symbol_name(sym_cls), sym_cls)
                for _, sym_cls in inspect.getmembers(
                    module_,
                    lambda v: inspect.isclass(v)
                    and issubclass(v, base_cls)
                    and not inspect.isabstract(v),
                )
            )

            # set module path when loading from file
            if module_name.endswith(".py"):
                for _, sym_cls in module_symbols:
                    sym_cls._classpath = f"{module_name}:{sym_cls.__name__}"

            # check for double symbols in the same module
            for idx, (sym_name, sym_cls) in enumerate(module_symbols):
                for other_sym_name, other_sym_cls in module_symbols[idx + 1 :]:  # noqa
                    if sym_name == other_sym_name:
                        cls._warn_double_def(
                            symbol_type, sym_name, sym_cls, other_sym_cls
                        )

            # check for double commands across modules
            module_symbols = dict(module_symbols)
            for sym_name, sym_cls in module_symbols.items():
                if sym_name in all_syms:
                    cls._warn_double_def(
                        symbol_type, sym_name, sym_cls, all_syms[sym_name]
                    )

            all_syms = {**all_syms, **module_symbols}
        return all_syms

    @classmethod
    def import_everything(cls):
        import pipelime.choixe.utils.imports as pl_imports
        import pipelime.sequences as pl_seq
        from pipelime.piper import PipelimeCommand
        from pipelime.stages import SampleStage

        if not cls.is_cache_valid():
            for module_name in cls.std_modules + cls.extra_modules:
                if module_name not in cls.cached_modules:
                    cls.cached_modules[module_name] = (
                        pl_imports.import_module(module_name)
                    )

            cls.cached_seq_ops = {
                (
                    "Sequence Generator",
                    "Sequence Generators",
                ): pl_seq.SamplesSequence._sources,
                (
                    "Sequence Piped Operation",
                    "Sequence Piped Operations",
                ): pl_seq.SamplesSequence._pipes,
            }

            cls.cached_cmds = {
                ("Pipelime Command", "Pipelime Commands"): cls._load_symbols(
                    PipelimeCommand, "command"
                )
            }

            cls.cached_stages = {
                ("Sample Stage", "Sample Stages"): cls._load_symbols(
                    SampleStage, "stage"
                )
            }

    @classmethod
    def get_pipelime_commands(cls):
        cls.import_everything()
        return cls.cached_cmds

    @classmethod
    def get_sequence_operators(cls):
        cls.import_everything()
        return cls.cached_seq_ops

    @classmethod
    def get_sample_stages(cls):
        cls.import_everything()
        return cls.cached_stages

    @classmethod
    def get_actions(cls):
        return cls.registered_actions

    @classmethod
    def get_symbol(
        cls,
        symbol_path: str,
        base_cls: t.Type,
        symbol_cache: t.Mapping[t.Tuple[str, str], t.Mapping],
    ):
        import pipelime.choixe.utils.imports as pl_imports

        if "." in symbol_path or ":" in symbol_path:
            try:
                imported_symbol = pl_imports.import_symbol(symbol_path)
                if not issubclass(imported_symbol, base_cls):
                    raise ValueError(
                        f"{symbol_path} must derived from {base_cls.__name__}."
                    )

                if ".py:" in symbol_path:
                    imported_symbol._classpath = symbol_path

                return imported_symbol
            except (ImportError, TypeError):
                return None

        for sym_type, sym_dict in symbol_cache.items():
            if symbol_path in sym_dict:
                return (sym_type, sym_dict[symbol_path])
        return None

    @classmethod
    def get_command(cls, command_name: str):
        from pipelime.piper import PipelimeCommand

        sym_cls = cls.get_symbol(
            command_name, PipelimeCommand, cls.get_pipelime_commands()
        )
        if sym_cls is None:
            return None
        if not isinstance(sym_cls, tuple):
            return (("Imported Command", "Imported Commands"), sym_cls)
        return sym_cls

    @classmethod
    def get_operator(cls, operator_name: str):
        for op_type, op_dict in PipelimeSymbolsHelper.get_sequence_operators().items():
            if operator_name in op_dict:
                return (op_type, op_dict[operator_name])
        return None

    @classmethod
    def get_stage(cls, stage_name: str):
        from pipelime.stages import SampleStage

        sym_cls = cls.get_symbol(stage_name, SampleStage, cls.get_sample_stages())
        if sym_cls is None:
            return None
        if not isinstance(sym_cls, tuple):
            return (("Imported Stage", "Imported Stages"), sym_cls)
        return sym_cls

    @classmethod
    def show_error_and_help(
        cls, name: str, should_be_cmd: bool, should_be_op: bool, should_be_stage: bool
    ):
        from pipelime.cli.pretty_print import print_error, print_warning, print_info
        from difflib import get_close_matches

        names_list = []
        if should_be_cmd:
            names_list += [
                v2 for v1 in cls.get_pipelime_commands().values() for v2 in v1.keys()
            ]
        if should_be_op:
            names_list += [
                v2 for v1 in cls.get_sequence_operators().values() for v2 in v1.keys()
            ]
        if should_be_stage:
            names_list += [
                v2 for v1 in cls.get_sample_stages().values() for v2 in v1.keys()
            ]
        similar_names = get_close_matches(name, names_list, cutoff=0.3)

        print_error(f"{name} is not a pipelime object and cannot be imported.")
        print_warning("Have you added the module with `--module/-m`?")
        if similar_names:
            print_info(f"Similar entries: {', '.join(similar_names)}")


def _print_info(
    info_cls,
    show_class_path=True,
    show_piper_port=True,
    show_description=True,
    recursive=True,
):
    from pipelime.cli.pretty_print import print_info, print_model_info, _short_line

    if info_cls is not None:
        print_info(f"\n{_short_line()} {info_cls[0][0]}")
        print_model_info(
            info_cls[1],
            show_class_path=show_class_path,
            show_piper_port=show_piper_port,
            show_description=show_description,
            recursive=recursive,
        )


def _unwrap_generic_type(symbol, verbose: bool):
    from pipelime.cli.pretty_print import print_debug
    from rich.markup import escape

    if verbose:
        symstr = escape(repr(symbol))
        for name in (
            "Callable",
            "Tuple",
            "List",
            "Dict",
            "Mapping",
            "Sequence",
            "Union",
            "Literal",
            "Final",
            "ClassVar",
        ):
            symstr = symstr.replace(name, f"[dark_red]{name}[/dark_red]")
        print_debug(f"Unwrapping [not italic]{symstr}[/]")
    all_types = set()
    if t.get_origin(symbol) is None:
        all_types.add(symbol)
    else:
        for arg in t.get_args(symbol):
            all_types.update(_unwrap_generic_type(arg, verbose))
    return all_types


def print_command_op_stage_info(
    command_operator_stage: str, show_description: bool = True, recursive: bool = True
):
    """
    Prints detailed info about a pipelime command, a sequence operator or a sample
    stage.
    """
    from pipelime.cli.pretty_print import print_info
    from pipelime.choixe.utils.imports import import_symbol

    available_defs = []

    try:
        available_defs.append(
            (
                PipelimeSymbolsHelper.get_stage(command_operator_stage),
                {"show_class_path": True, "show_piper_port": False},
            )
        )
    except ValueError:
        pass

    available_defs.append(
        (
            PipelimeSymbolsHelper.get_operator(command_operator_stage),
            {"show_class_path": False, "show_piper_port": False},
        )
    )

    try:
        available_defs.append(
            (
                PipelimeSymbolsHelper.get_command(command_operator_stage),
                {"show_class_path": True, "show_piper_port": True},
            )
        )
    except ValueError:
        pass

    # Remove None entries, if any
    available_defs = [x for x in available_defs if x[0] is not None]

    if not available_defs:
        # Try a generic import
        try:
            sym = import_symbol(command_operator_stage)
        except ImportError:
            pass
        else:
            sym = _unwrap_generic_type(sym, verbose=True)
            available_defs.extend(
                (
                    (
                        ("Imported Symbol", "Imported Symbols"),
                        s,
                    ),
                    {"show_class_path": True, "show_piper_port": False},
                )
                for s in sym
            )

    if not available_defs:
        PipelimeSymbolsHelper.show_error_and_help(
            command_operator_stage,
            should_be_cmd=True,
            should_be_op=True,
            should_be_stage=True,
        )
        raise ValueError(
            f"{command_operator_stage} is not a pipelime object and cannot be imported."
        )

    if len(available_defs) > 1:
        from rich.prompt import Prompt
        from rich.markup import escape

        print_info("Multiple definitions found!")
        idx = Prompt.ask(
            ">>> Show:\n"
            + "\n".join(
                escape(f"[{i:>2}] {v[0][0][0]} {v[0][1]}")
                for i, v in enumerate(available_defs)
            )
            + "\n[-1] ALL\n>>>",
            choices=["-1"] + list(str(v) for v in range(len(available_defs))),
            default="-1",
        )
        idx = int(idx)
        if idx >= 0:
            available_defs = [available_defs[idx]]

    for d in available_defs:
        _print_info(
            d[0], **d[1], show_description=show_description, recursive=recursive
        )


def _print_details(info, show_class_path, show_piper_port, show_description, recursive):
    from pipelime.cli.pretty_print import _short_line, print_info, print_model_info

    for info_type, info_map in info.items():
        for info_cls in info_map.values():
            print_info(f"\n{_short_line()} {info_type[0]}")
            print_model_info(
                info_cls,
                show_class_path=show_class_path,
                show_piper_port=show_piper_port,
                show_description=show_description,
                recursive=recursive,
            )


def _print_short_help(info, show_class_path, *args, **kwargs):
    from pipelime.cli.pretty_print import (
        _short_line,
        print_info,
        print_models_short_help,
    )

    for info_type, info_map in info.items():
        print_info(f"\n{_short_line()} {info_type[1]}")
        print_models_short_help(
            *[info_cls for info_cls in info_map.values()],
            show_class_path=show_class_path,
        )


def print_commands_ops_stages_list(
    show_details: bool = False,
    *,
    show_cmds: bool = True,
    show_ops: bool = True,
    show_stages: bool = True,
    show_description: bool = True,
    recursive: bool = True,
):
    """Print a list of all available sequence operators and pipelime commands."""
    from pipelime.cli.pretty_print import (
        get_model_classpath,
        print_info,
        print_actions_short_help,
        _short_line,
    )

    def _filter_symbols(smbls):
        if not PipelimeSymbolsHelper.extra_modules:
            return smbls
        filtered_smbls = {}
        for k, v in smbls.items():
            vfilt = {}
            for sym_name, sym_cls in v.items():
                if not get_model_classpath(sym_cls).startswith("pipelime."):
                    vfilt[sym_name] = sym_cls
            if vfilt:
                filtered_smbls[k] = vfilt
        return filtered_smbls

    print_fn = _print_details if show_details else _print_short_help
    if show_cmds:
        print_fn(
            _filter_symbols(PipelimeSymbolsHelper.get_pipelime_commands()),
            show_class_path=True,
            show_piper_port=True,
            show_description=show_description,
            recursive=recursive,
        )
    if show_ops:
        print_fn(
            _filter_symbols(PipelimeSymbolsHelper.get_sequence_operators()),
            show_class_path=False,
            show_piper_port=False,
            show_description=show_description,
            recursive=recursive,
        )
    if show_stages:
        print_fn(
            _filter_symbols(PipelimeSymbolsHelper.get_sample_stages()),
            show_class_path=True,
            show_piper_port=False,
            show_description=show_description,
            recursive=recursive,
        )
        if PipelimeSymbolsHelper.get_actions():
            print_info(f"\n{_short_line()} Actions")
            print_actions_short_help(
                *PipelimeSymbolsHelper.get_actions().values(), show_class_path=True
            )


def print_commands_list(show_details: bool = False):
    """Print a list of all available pipelime commands."""
    print_commands_ops_stages_list(
        show_details=show_details, show_cmds=True, show_ops=False, show_stages=False
    )


def print_sequence_operators_list(show_details: bool = False):
    """Print a list of all available sequence operators."""
    print_commands_ops_stages_list(
        show_details=show_details, show_cmds=False, show_ops=True, show_stages=False
    )


def print_sample_stages_list(show_details: bool = False):
    """Print a list of all available sample stages."""
    print_commands_ops_stages_list(
        show_details=show_details, show_cmds=False, show_ops=False, show_stages=True
    )


def pl_print(
    *objects: t.Any,
    sep: str = " ",
    end: str = "\n",
    file: t.Optional[t.IO[str]] = None,
    flush: bool = False,
):
    """Prints Items, Samples, Sequences, commands, stages, operations..."""
    import inspect
    from rich import print as rprint
    from pydantic import BaseModel
    from pipelime.cli.pretty_print import print_model_info
    from pipelime.sequences import SamplesSequence
    from pipelime.piper import PipelimeCommand

    for obj in objects:
        if isinstance(obj, (str, bytes)):
            if isinstance(obj, bytes):
                obj = obj.decode("utf-8")
            print_command_op_stage_info(obj)
        elif inspect.isclass(obj) and issubclass(obj, BaseModel):
            print_model_info(
                obj,
                show_class_path=(not issubclass(obj, SamplesSequence)),
                show_piper_port=issubclass(obj, PipelimeCommand),
            )
        else:
            sobj = (
                obj.__pl_pretty__()  # type: ignore
                if hasattr(obj, "__pl_pretty__")
                else str(obj)
            )
            rprint(sobj, sep=sep, end=end, file=file, flush=flush)


def create_stage_from_config(
    stage_name: str, stage_args: t.Union[t.Mapping[str, t.Any], t.Sequence, None]
) -> "SampleStage":  # type: ignore # noqa: E602,F821
    """Creates a stage from a name and arguments.

    :param stage_name: the name of the stage, eg, `compose`, `remap`, etc.
    :type stage_name: str
    :param stage_args: a mapping of the stage init arguments.
    :type stage_args: t.Mapping[str, t.Any]]
    :return: the stage object
    :rtype: SampleStage
    """
    from pipelime.cli.utils import PipelimeSymbolsHelper
    from pipelime.stages import SampleStage

    stage_cls = PipelimeSymbolsHelper.get_stage(stage_name)
    if stage_cls is None or not issubclass(stage_cls[1], SampleStage):
        PipelimeSymbolsHelper.show_error_and_help(
            stage_name,
            should_be_cmd=False,
            should_be_op=False,
            should_be_stage=True,
        )
        raise ValueError(f"{stage_name} is not a pipelime stage.")
    stage_cls = stage_cls[1]
    if stage_args is None:
        return stage_cls()
    try:
        if isinstance(stage_args, (str, bytes)) or not isinstance(
            stage_args, (t.Sequence, t.Mapping)
        ):
            stage_args = [stage_args]
        return (
            stage_cls(**stage_args)
            if isinstance(stage_args, t.Mapping)
            else stage_cls(*stage_args)
        )
    except TypeError:
        # try to call without expanding args
        return stage_cls(stage_args)


def get_pipelime_command_cls(
    cmd_name: str, interactive: bool = True
) -> t.Type["PipelimeCommand"]:
    from pipelime.piper.model import PipelimeCommand

    cmd_cls = PipelimeSymbolsHelper.get_command(cmd_name)
    if cmd_cls is None or not issubclass(cmd_cls[1], PipelimeCommand):
        if interactive:
            PipelimeSymbolsHelper.show_error_and_help(
                cmd_name, should_be_cmd=True, should_be_op=False, should_be_stage=False
            )
        raise ValueError(f"{cmd_name} is not a pipelime command.")
    return cmd_cls[1]


def get_pipelime_command(
    cmd: t.Union[t.Mapping[str, t.Optional[t.Mapping[str, t.Any]]], "PipelimeCommand"]
) -> "PipelimeCommand":
    from pipelime.piper.model import PipelimeCommand

    if isinstance(cmd, PipelimeCommand):
        return cmd

    cmd_name, cmd_args = next(iter(cmd.items()))
    cmd_cls = get_pipelime_command_cls(cmd_name)
    return cmd_cls() if cmd_args is None else cmd_cls(**cmd_args)


def time_to_str(nanosec: int) -> str:
    from datetime import timedelta

    if nanosec < 1000000000:
        millisec = int(nanosec // 1000000)
        nanosec -= millisec * 1000000
        microsec = int(nanosec // 1000)
        nanosec -= microsec * 1000
        return f"{millisec}ms {microsec}us {nanosec}ns"
    return str(timedelta(microseconds=nanosec / 1000))
