import typing as t

import typer
from rich import print as rprint


def _pinfo(val):
    rprint("[cyan]", val, "[/]", sep="")


def _pwarn(val):
    rprint("[orange1][bold blink]WARNING:[/bold blink]", val, "[/orange1]")


def _perr(val):
    rprint(
        "[dark_red on white][bold blink]ERROR:[/bold blink]",
        val,
        "[/dark_red on white]",
    )


class _Helper:
    std_cmd_modules = ["pipelime.commands"]
    extra_modules: t.List[str] = []

    cached_nodes: t.Optional[t.Dict] = None
    cached_seq_ops: t.Optional[t.Tuple[t.Dict, t.Dict]] = None

    @classmethod
    def get_sequence_operations(cls):
        import pipelime.choixe.utils.imports as pl_imports
        import pipelime.sequences as pls

        if cls.cached_seq_ops is None:
            _ = [
                pl_imports.import_module_from_file(module_name)
                if module_name.endswith(".py")
                else pl_imports.import_module_from_path(module_name)
                for module_name in cls.extra_modules
            ]
            cls.cached_seq_ops = (
                pls.SamplesSequence._sources,
                pls.SamplesSequence._pipes,
            )

        return cls.cached_seq_ops

    @classmethod
    def get_piper_nodes(cls):
        import inspect

        import pipelime.choixe.utils.imports as pl_imports
        from pipelime.piper import PipelimeCommand

        if cls.cached_nodes is None:
            all_modules = cls.std_cmd_modules + list(cls.extra_modules)
            all_nodes = {}
            for module_name in all_modules:
                module_ = (
                    pl_imports.import_module_from_file(module_name)
                    if module_name.endswith(".py")
                    else pl_imports.import_module_from_path(module_name)
                )
                module_nodes = {
                    node_cls.command_title(): node_cls
                    for _, node_cls in inspect.getmembers(
                        module_,
                        lambda v: inspect.isclass(v)
                        and issubclass(v, PipelimeCommand)
                        and v is not PipelimeCommand,
                    )
                }
                all_nodes = {**all_nodes, **module_nodes}
            cls.cached_nodes = all_nodes
        return cls.cached_nodes

    @classmethod
    def get_operation_or_die(cls, op_name: str):
        ops = _Helper.get_sequence_operations()
        if op_name not in ops[0] and op_name not in ops[1]:
            _pwarn(f"{op_name} is not a registered operation!")
            _pinfo("Have you added the module with `--module`?")
            raise typer.Exit()
        return ops[0].get(op_name) or ops[1].get(op_name)

    @classmethod
    def get_node_or_die(cls, node_name: str):
        import pipelime.choixe.utils.imports as pl_imports

        if "." in node_name or ":" in node_name:
            return pl_imports.import_symbol(node_name)

        nodes = _Helper.get_piper_nodes()
        if node_name not in nodes:
            _pwarn(f"{node_name} is not a Piper node!")
            _pinfo("Have you added the module with `--module`?")
            raise typer.Exit()
        return nodes[node_name]


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
    _Helper.extra_modules = extra_modules


@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def run(
    node: str = typer.Argument(
        ...,
        help=(
            "The Piper node to run, ie, a `command`, a `package.module.ClassName` "
            "class path or a `path/to/module.py:ClassName` uri."
        ),
    ),
    ctx: typer.Context = typer.Option(None),
):
    """
    Run a piper command.
    """
    import pydash as py_

    node_cls = _Helper.get_node_or_die(node)

    last_opt = None
    last_val = None
    all_opts = {}

    def _store_opt():
        if last_opt is not None:
            val = last_val if last_val is not None else True

            curr_val = py_.get(all_opts, last_opt, None)
            if curr_val is None:
                py_.set_(all_opts, last_opt, val)
            else:
                if not isinstance(curr_val, list):
                    curr_val = [curr_val]
                py_.set_(all_opts, last_opt, [val] + curr_val)

    for extra_arg in ctx.args:
        if extra_arg.startswith("--"):
            _store_opt()
            last_opt, _, last_val = extra_arg[2:].partition("=")
            last_val = None if not last_val else _convert_val(last_val)
        elif last_val is None:
            last_val = _convert_val(extra_arg)
        else:
            if not isinstance(last_val, tuple):
                last_val = (last_val,)
            last_val += (_convert_val(extra_arg),)
    _store_opt()

    _pinfo(f"Creating `{node}` node with options:")
    _pinfo(all_opts)

    node_obj = node_cls(**all_opts)

    _pinfo("\nCreated node:")
    _pinfo(node_obj.dict())

    _pinfo("\nRunning...")
    node_obj()


@app.command("list")
def list_nodes_and_ops(
    seq: bool = typer.Option(
        True,
        help="Show the available generators and operations on samples sequences.",
    ),
    cmd: bool = typer.Option(
        True,
        help="Show the available piper commands.",
    ),
    details: bool = typer.Option(
        False,
        "--details",
        "-d",
        help="Show a complete field description for each command and each operation.",
    ),
):
    """
    List all available samples sequence's operations and piper commands.
    """
    from pipelime.cli.pretty_print import print_node_names, print_node_info

    if details:
        if cmd:
            for node_cls in _Helper.get_piper_nodes().values():
                print("---Piper Command")
                print_node_info(node_cls)
        if seq:
            desc = ("---Sequence Generator", "---Sequence Operation")
            for op_map, d in zip(_Helper.get_sequence_operations(), desc):
                for op_cls in op_map.values():
                    print(d)
                    print_node_info(
                        op_cls, show_class_path=False
                    )
    else:
        if cmd:
            print("---Piper Commands")
            print_node_names(*_Helper.get_piper_nodes().values())
        if seq:
            desc = ("---Sequence Generators", "---Sequence Operations")
            for op_map, d in zip(_Helper.get_sequence_operations(), desc):
                print(d)
                print_node_names(
                    *[op_cls for op_cls in op_map.values()], show_class_path=False
                )


@app.command("info")
def node_info(
    node: str = typer.Argument(
        ...,
        help=(
            "The piper command, ie, a `command-name`, a `package.module.ClassName` "
            "class path or a `path/to/module.py:ClassName` uri."
        ),
    )
):
    """
    Get info about a piper command.
    """
    from pipelime.cli.pretty_print import print_node_info

    node_cls = _NodeHelper.get_node_or_die(node)
    print_node_info(node_cls)
