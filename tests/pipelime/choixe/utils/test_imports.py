from pathlib import Path

import pipelime.choixe as choixe
import pytest
from pipelime.choixe.utils.imports import import_symbol


def test_import_symbol_fn():
    res = import_symbol("numpy.array")
    from numpy import array

    assert res is array


def test_import_symbol_cls():
    res = import_symbol("rich.console.Console")
    assert res.__name__ == "Console"

    from rich.console import Console

    assert res is Console


def test_import_symbol_path(choixe_folder: Path):
    import inspect

    file_path = choixe_folder / "extra_module.py"
    res = import_symbol(f"{str(file_path)}:add_ten")
    assert res.__name__ == "add_ten"
    assert inspect.isfunction(res)
    assert res(10) == 20


def test_import_symbol_raise():
    # Non existing module
    with pytest.raises(ImportError):
        import_symbol("rich.console.SomethingThatDoesNotExist")

    # Non existing path
    with pytest.raises(ImportError):
        import_symbol("file_that_does_not_exist.py:AAAAAA")

    # Non existing object
    file_path = Path(choixe.__file__).parent / "ast" / "nodes.py"
    with pytest.raises(ImportError):
        import_symbol(f"{str(file_path)}:HolyPinoly")
