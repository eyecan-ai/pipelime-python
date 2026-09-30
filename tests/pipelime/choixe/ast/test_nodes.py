import pytest

import pipelime.choixe.ast.nodes as ast


class TestNodePostInit:
    """Regression tests for the construction-time type check restored in
    `Node.__post_init__` after moving the AST nodes off pydantic dataclasses
    onto stdlib dataclasses (which do not validate field types on their own).
    """

    def test_direct_construction_wrong_type_raises_typeerror(self):
        # VarNode.identifier is typed as HashNode; an ImportNode is a Node
        # but not a HashNode.
        with pytest.raises(TypeError):
            ast.VarNode(identifier=ast.ImportNode(path=ast.LiteralNode(data="x")))

    def test_dict_form_wrong_type_raises_typeerror(self):
        # InstanceNode.symbol is typed as HashNode.
        with pytest.raises(TypeError):
            ast.InstanceNode(
                symbol=ast.ImportNode(path=ast.LiteralNode(data="x")),
                args=ast.DictNode(nodes={}),
            )

    def test_optional_field_accepts_none(self):
        node = ast.VarNode(
            identifier=ast.LiteralNode(data="x"),
            default=None,
            env=None,
            help=None,
        )
        assert node.default is None
        assert node.env is None
        assert node.help is None

    def test_valid_construction_hash_and_equality_unaffected(self):
        a = ast.VarNode(
            identifier=ast.LiteralNode(data="x"), default=ast.LiteralNode(data=1)
        )
        b = ast.VarNode(
            identifier=ast.LiteralNode(data="x"), default=ast.LiteralNode(data=1)
        )
        assert a == b
        assert hash(a) == hash(b)

    def test_custom_init_nodes_stay_unchecked(self):
        # ListNode/DictBundleNode/StrBundleNode/SweepNode/RandNode all define
        # their own __init__ and were never validated, even under pydantic v1.
        wrong = ast.ImportNode(path=ast.LiteralNode(data="x"))
        ast.ListNode(wrong)
        ast.DictBundleNode(wrong)
        ast.SweepNode(wrong)
        ast.RandNode(wrong)
