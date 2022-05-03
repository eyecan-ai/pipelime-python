from typing import Any, Dict, List, Union

from pipelime.choixe.ast.nodes import (
    CmdNode,
    DateNode,
    DictBundleNode,
    DictNode,
    ForNode,
    ImportNode,
    IndexNode,
    InstanceNode,
    ItemNode,
    ListNode,
    LiteralNode,
    ModelNode,
    Node,
    NodeVisitor,
    StrBundleNode,
    SweepNode,
    TmpDirNode,
    UuidNode,
    VarNode,
)
from pipelime.choixe.ast.parser import DIRECTIVE_PREFIX


class Unparser(NodeVisitor):
    """`NodeVisitor` for the `unparse` operation."""

    def visit_dict(self, node: DictNode) -> Dict:
        data = {}
        for k, v in node.nodes.items():
            data[k.accept(self)] = v.accept(self)
        return data

    def visit_list(self, node: ListNode) -> List:
        data = []
        for x in node.nodes:
            data.append(x.accept(self))
        return data

    def visit_object(self, node: LiteralNode) -> Any:
        return node.data

    def visit_dict_bundle(self, node: DictBundleNode) -> Any:
        data = {}
        for x in node.nodes:
            data.update(x.accept(self))
        return data

    def visit_str_bundle(self, node: StrBundleNode) -> str:
        return "".join(x.accept(self) for x in node.nodes)

    def visit_sweep(self, node: SweepNode) -> Union[Dict, str]:
        return self._unparse_auto("sweep", *node.cases)

    def visit_var(self, node: VarNode) -> Union[Dict, str]:
        kwargs = {}
        if node.default is not None:
            kwargs["default"] = node.default
        if node.env is not None:
            kwargs["env"] = node.env
        return self._unparse_auto("var", node.identifier, **kwargs)

    def visit_import(self, node: ImportNode) -> Union[Dict, str]:
        return self._unparse_auto("import", node.path)

    def visit_instance(self, node: InstanceNode) -> Dict[str, Any]:
        return {
            self._unparse_compact("call"): node.symbol.accept(self),
            self._unparse_compact("args"): node.args.accept(self),
        }

    def visit_model(self, node: ModelNode) -> Dict[str, Any]:
        return {
            self._unparse_compact("model"): node.symbol.accept(self),
            self._unparse_compact("args"): node.args.accept(self),
        }

    def visit_for(self, node: ForNode) -> Dict[str, Any]:
        args = []
        if node.identifier is not None:
            args.append(node.identifier)
        key = self._unparse_call("for", node.iterable, *args)
        value = node.body.accept(self)
        return {key: value}

    def visit_item(self, node: ItemNode) -> Union[Dict, str]:
        args = []
        if node.identifier is not None:
            args.append(node.identifier)
        return self._unparse_auto("item", *args)

    def visit_index(self, node: IndexNode) -> Union[Dict, str]:
        args = []
        if node.identifier is not None:
            args.append(node.identifier)
        return self._unparse_auto("index", *args)

    def visit_uuid(self, node: UuidNode) -> str:
        return self._unparse_compact("uuid")

    def visit_date(self, node: DateNode) -> Union[Dict, str]:
        args = []
        if node.format is not None:
            args.append(node.format)
        return self._unparse_auto("date", *args)

    def visit_cmd(self, node: CmdNode) -> Union[Dict, str]:
        return self._unparse_auto("cmd", node.command)

    def visit_tmp_dir(self, node: TmpDirNode) -> Union[Dict, str]:
        args = []
        if node.name is not None:
            args.append(node.name)
        return self._unparse_auto("tmp", *args)

    def _unparse_as_arg(self, node: Node) -> str:
        unparsed = node.accept(self)
        if isinstance(unparsed, str):
            if all([x.isidentifier() for x in unparsed.split(".")]):
                return unparsed
            else:
                return f'"{unparsed}"'
        else:
            return str(unparsed)

    def _unparse_as_args(self, *nodes: Node) -> str:
        return ", ".join([self._unparse_as_arg(x) for x in nodes])

    def _unparse_as_kwargs(self, **nodes: Node) -> str:
        return ", ".join([f"{k}={self._unparse_as_arg(v)}" for k, v in nodes.items()])

    def _unparse_compact(self, name: str) -> str:
        return f"{DIRECTIVE_PREFIX}{name}"

    def _unparse_call(self, name: str, *args: Node, **kwargs: Node) -> str:
        parts = []
        if len(args) > 0:
            parts.append(self._unparse_as_args(*args))
        if len(kwargs) > 0:
            parts.append(self._unparse_as_kwargs(**kwargs))
        return f"{self._unparse_compact(name)}({', '.join(parts)})"

    def _unparse_extended(
        self, name: str, *args: Node, **kwargs: Node
    ) -> Dict[str, Any]:
        return {
            "$directive": name,
            "$args": [x.accept(self) for x in args],
            "$kwargs": {k: v.accept(self) for k, v in kwargs.items()},
        }

    def _unparse_auto(self, name: str, *args: Node, **kwargs: Node) -> Union[Dict, str]:
        if len(args) == 0 and len(kwargs) == 0:
            return self._unparse_compact(name)

        all_leafs = True
        for x in list(args) + list(kwargs.values()):
            if not isinstance(x, LiteralNode):
                all_leafs = False
                break

        if all_leafs:
            return self._unparse_call(name, *args, **kwargs)

        return self._unparse_extended(name, *args, **kwargs)


def unparse(node: Node) -> Any:
    """Visitor that undoes the parsing operation.

    If a parser transforms an object into an AST Node, the unparser transforms the AST
    Node back to an object that can be parsed into the same AST Node, as close as
    possible to the original object.

    Of course, since the parser discards some irrelevant information like unnecessary
    white spaces around tokens, the unparser cannot magically bring them back.
    For example::

        unparse(parse('$var( my_id  ,default= 10       '))

    results in::

        '$var(my_id, default=10)'

    Args:
        node (Node): The AST node to unparse.

    Returns:
        Any: The unparsed object.
    """
    unparser = Unparser()
    return node.accept(unparser)
