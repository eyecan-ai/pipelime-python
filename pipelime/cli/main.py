import typer
import typing as t


class _NodeHelper:
    std_modules = ["pipelime.commands"]
    additional_modules: t.List[str] = []
    cached_nodes: t.Optional[t.Dict] = None

    @classmethod
    def get_piper_nodes(cls):
        import inspect
        import pipelime.choixe.utils.imports as pl_imports
        from pipelime.piper import PipelimeCommand

        if cls.cached_nodes is None:
            all_modules = cls.std_modules + list(cls.additional_modules)
            all_nodes = {}
            for module_name in all_modules:
                module_ = (
                    pl_imports.import_module_from_file(module_name)
                    if module_name.endswith(".py")
                    else pl_imports.import_module_from_path(module_name)
                )
                module_nodes = {
                    node_cls.get_node_name(): node_cls
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
    def get_node_or_die(cls, node_name: str):
        import pipelime.choixe.utils.imports as pl_imports

        if "." in node_name or ":" in node_name:
            return pl_imports.import_symbol(node_name)

        nodes = _NodeHelper.get_piper_nodes()
        if node_name not in nodes:
            typer.secho(
                f"WARNING: {node_name} is not a Piper node!", fg="white", bg="red"
            )
            typer.secho("Have you added the module with `--module`?", bold=True)
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
    additional_modules: t.List[str] = typer.Option(
        [], "--module", "-m", help="Additional modules to import."
    )
):
    """
    Pipelime Command Line Interface
    """
    _NodeHelper.additional_modules = additional_modules


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
    Run a Piper node
    """
    import pydash as py_

    node_cls = _NodeHelper.get_node_or_die(node)

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

    typer.echo(f"Creating `{node}` node with options:")
    typer.echo(all_opts)

    node_obj = node_cls(**all_opts)

    typer.echo("Created node:")
    typer.echo(node_obj.dict())

    typer.echo("Running...")
    node_obj()


@app.command("list")
def list_nodes():
    """
    List all available Piper nodes
    """
    for node_cls in _NodeHelper.get_piper_nodes().values():
        typer.echo(f"\n{node_cls.pretty_schema()}")


@app.command("info")
def node_info(
    node: str = typer.Argument(
        ...,
        help=(
            "The Piper node, ie, a `command`, a `package.module.ClassName` "
            "class path or a `path/to/module.py:ClassName` uri."
        ),
    )
):
    node_cls = _NodeHelper.get_node_or_die(node)
    typer.echo(node_cls.pretty_schema())
