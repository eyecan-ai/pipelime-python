import ast
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, OrderedDict, Tuple, Type, Union

import astunparse
from schema import Schema

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
    StrBundleNode,
    SweepNode,
    SymbolNode,
    TmpDirNode,
    UuidNode,
    VarNode,
)

DIRECTIVE_PREFIX = "$"
"""Prefix used at the start of all Choixe directives."""


class ChoixeParsingError(Exception):
    pass


class ChoixeSyntaxError(ChoixeParsingError):
    pass


class ChoixeTokenValidationError(ChoixeParsingError):
    pass


class ChoixeStructValidationError(ChoixeParsingError):
    pass


@dataclass
class Token:
    name: str
    args: List[Any]
    kwargs: Dict[str, Any]


class Scanner:
    """Choixe Scanner of python str objects."""

    DIRECTIVE_RE = (
        rf"(?:\$[^\)\( \.\,\$]+\([^\(\)]*\))|(?:\$[^\)\( \.\,\$]+)|(?:[^\$]*)"
    )
    """Regex used to check if a string is a Choixe directive."""

    def _scan_argument(
        self, py_arg: Union[ast.Constant, ast.Attribute, ast.Name]
    ) -> Any:
        if isinstance(py_arg, ast.Constant):
            return py_arg.value
        elif isinstance(py_arg, ast.Attribute):
            name = astunparse.unparse(py_arg)
            if name.endswith("\n"):  # Remove trailing newline
                name = name[:-1]
            return str(name)
        elif isinstance(py_arg, ast.Name):
            return str(py_arg.id)
        else:
            raise ChoixeSyntaxError(py_arg.__class__)

    def _scan_directive(self, code: str) -> Token:
        try:
            py_ast = ast.parse(f"_{code}")  # Add "_" to avoid conflicts with python
        except SyntaxError as e:
            raise ChoixeSyntaxError(code)

        assert isinstance(py_ast, ast.Module)
        py_expr = py_ast.body[0].value  # type: ignore

        if isinstance(py_expr, ast.Call):
            # Remove the added "_"
            token_name = py_expr.func.id[1:]  # type: ignore
            py_args = py_expr.args
            py_kwargs = py_expr.keywords
        elif isinstance(py_expr, ast.Name):
            token_name = py_expr.id[1:]
            py_args = []
            py_kwargs = {}
        else:
            raise ChoixeSyntaxError(code)

        args = []
        for py_arg in py_args:
            args.append(self._scan_argument(py_arg))  # type: ignore

        kwargs = {}
        for py_kwarg in py_kwargs:
            key, value = py_kwarg.arg, py_kwarg.value
            kwargs[key] = self._scan_argument(value)

        return Token(token_name, args, kwargs)

    def scan(self, data: str) -> List[Token]:
        """Transforms a string into a list of parsed tokens.

        Args:
            data (str): The string to parse.

        Returns:
            List[Token]: The list of parsed tokens.
        """
        res = []
        tokens = re.findall(self.DIRECTIVE_RE, data)
        for token in tokens:
            if len(token) == 0:
                continue
            if token.startswith(DIRECTIVE_PREFIX):
                token = self._scan_directive(token[len(DIRECTIVE_PREFIX) :])
            else:
                token = Token("str", [token], {})
            res.append(token)

        return res


class Parser:
    """Choixe parser for all kind of python objects."""

    def __init__(self) -> None:
        self._scanner = Scanner()
        self._type_map = {
            dict: self._parse_dict,
            list: self._parse_list,
            tuple: self._parse_list,
            str: self._parse_str,
        }

        self._call_forms = {
            self._token_schema("var"): VarNode,
            self._token_schema("import"): ImportNode,
            self._token_schema("sweep"): SweepNode,
            self._token_schema("index"): IndexNode,
            self._token_schema("item"): ItemNode,
            self._token_schema("uuid"): UuidNode,
            self._token_schema("date"): DateNode,
            self._token_schema("cmd"): CmdNode,
            self._token_schema("tmp"): TmpDirNode,
            self._token_schema("symbol"): SymbolNode,
        }

        self._extended_and_special_forms = {
            Schema(
                {
                    self._token_schema("directive"): str,
                    self._token_schema("args"): list,
                    self._token_schema("kwargs"): dict,
                }
            ): self._parse_extended_form,
            Schema(
                {self._token_schema("call"): str, self._token_schema("args"): dict}
            ): self._parse_instance,
            Schema(
                {self._token_schema("model"): str, self._token_schema("args"): dict}
            ): self._parse_model,
        }

        self._key_value_forms = {
            Schema({self._token_schema("for"): object}): self._parse_for
        }

    def _token_schema(
        self,
        name: str,
        args: Optional[OrderedDict[str, Type]] = None,
        kwargs: Optional[OrderedDict[str, Type]] = None,
    ) -> Schema:
        args = OrderedDict() if args is None else args
        kwargs = OrderedDict() if kwargs is None else kwargs

        def _validator(token: Union[str, Token]) -> bool:
            if isinstance(token, str):
                tokens = self._scanner.scan(token)
                if len(tokens) != 1:
                    return False
                token = tokens[0]

            if token.name != name:
                return False

            return True

        return Schema(_validator)

    def _key_value_pairs_by_token_name(
        self, data: Dict[str, Any]
    ) -> Dict[str, Tuple[Token, Any]]:
        res = {}
        for k, v in data.items():
            token = self._scanner.scan(k)[0]
            res[token.name] = (token, v)
        return res

    def _parse_extended_form(self, data: dict) -> Node:
        pairs = self._key_value_pairs_by_token_name(data)
        token = Token(pairs["directive"][1], pairs["args"][1], pairs["kwargs"][1])
        return self._parse_token(token)

    def _parse_instance(self, data: dict) -> InstanceNode:
        pairs = self._key_value_pairs_by_token_name(data)
        symbol = LiteralNode(pairs["call"][1])
        args = self.parse(pairs["args"][1])
        return InstanceNode(symbol, args)

    def _parse_model(self, data: dict) -> ModelNode:
        pairs = self._key_value_pairs_by_token_name(data)
        symbol = LiteralNode(pairs["model"][1])
        args = self.parse(pairs["args"][1])
        return ModelNode(symbol, args)

    def _parse_for(self, loop: str, body: Any) -> ForNode:
        loop = self._scanner.scan(loop)[0]
        iterable = LiteralNode(loop.args[0])
        identifier = (
            loop.args[1] if len(loop.args) > 1 else loop.kwargs.get("identifier")
        )
        identifier = LiteralNode(identifier) if identifier else None
        return ForNode(iterable, self.parse(body), identifier=identifier)

    def _parse_dict(self, data: dict) -> DictNode:
        # Check if the dict is an extended or special form, in that case parse it and
        # return the result, no further checks are needed.
        for schema, fn in self._extended_and_special_forms.items():
            if schema.is_valid(data):
                try:
                    return fn(data)
                except:
                    raise ChoixeStructValidationError(data)

        # Check for each entry if it is a key_value form, in that case parse it and add
        # it to the bundle. Keep track of what is left to parse.
        parsed_keyvalues = []
        parsed_other = {}
        for schema, fn in self._key_value_forms.items():
            for k, v in data.items():
                if schema.is_valid({k: v}):
                    parsed_keyvalues.append(fn(k, v))
                else:
                    parsed_other[self._parse_str(k)] = self.parse(v)

        # Parse the remaining entries as a DictNode.
        parsed_other = DictNode(parsed_other)

        # If no key_value form was found, return the DictNode.
        if len(parsed_keyvalues) == 0:
            return parsed_other
        # If there is only one key_value form and no remaining data, return it.
        elif len(parsed_other.nodes) == 0 and len(parsed_keyvalues) == 1:
            return parsed_keyvalues[0]
        # If there is more than one key_value form, return a DictBundleNode.
        else:
            return DictBundleNode(*parsed_keyvalues, parsed_other)

    def _parse_list(self, data: list) -> ListNode:
        return ListNode(*[self.parse(x) for x in data])

    def _parse_token(self, token: Token) -> Node:
        if token.name == "str":
            return LiteralNode(token.args[0])

        for schema, fn in self._call_forms.items():
            if schema.is_valid(token):
                args = [self.parse(x) for x in token.args]
                kwargs = {k: self.parse(v) for k, v in token.kwargs.items()}
                node = fn(*args, **kwargs)
                return node

        raise ChoixeTokenValidationError(token)

    def _parse_str(self, data: str) -> Node:
        nodes = []
        for token in self._scanner.scan(data):
            nodes.append(self._parse_token(token))

        if len(nodes) == 1:
            return nodes[0]
        else:
            return StrBundleNode(*nodes)

    def parse(self, data: Any) -> Node:
        """Recursively transforms an object into a visitable AST node.

        Args:
            data (Any): The object to parse.

        Returns:
            Node: The parsed Choixe AST node.
        """
        try:
            fn = LiteralNode
            for type_, parse_fn in self._type_map.items():
                if isinstance(data, type_):
                    fn = parse_fn
                    break
            res = fn(data)
            return res

        except (ChoixeSyntaxError, SyntaxError) as e:
            code = e.args[0]
            raise ChoixeParsingError(
                f'Error when parsing code "{code}" found in "{data}", expected either '
                "a compact form like $DIRECTIVE, or a call form like "
                "$DIRECTIVE(ARGS, KWARGS)."
            )
        except ChoixeTokenValidationError as e:
            token: Token = e.args[0]
            raise ChoixeParsingError(
                f'Token "{token}" found in "{data}" does not validate against any of '
                "the available call forms. Please check that the directive and "
                "argument names are spelled correctly and match the directive "
                "signature."
            )
        except ChoixeStructValidationError as e:
            expr = e.args[0]
            raise ChoixeParsingError(
                f'Structure "{expr}" does not validate against any of the available '
                "special or extended forms. Please check that the directive and "
                "argument names are spelled correctly and match the directive "
                "signature."
            )


def parse(data: Any) -> Node:
    """Recursively transforms an object into a visitable AST node.

    Args:
        data (Any): The object to parse.

    Returns:
        Node: The parsed Choixe AST node.
    """
    return Parser().parse(data)
