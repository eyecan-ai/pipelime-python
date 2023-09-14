import importlib
import importlib.util
import sys
from contextlib import ContextDecorator
from pathlib import Path
from types import ModuleType
from typing import Any, Optional, Union


class add_to_sys_path(ContextDecorator):
    """add_to_sys_path context decorator temporarily adds a path to sys.path"""

    def __init__(self, path: str) -> None:
        self._new_cwd = path

    def __enter__(self) -> None:
        sys.path.insert(0, self._new_cwd)

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        sys.path.pop(0)


class add_to_sys_module(ContextDecorator):
    """
    ☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️
    THIS IS A HACK TO ALLOW DYNAMIC IMPORT FROM FILES CONTAINING DATACLASSES.
    THE USAGE OF THIS DECORATOR SHOULD BE AVOIDED AS IT COULD POTENTIALLY BREAK STUFF.

    THIS DECORATOR IS CURRENTLY USED ONLY IN THIS MODULE AND WE PLAN TO REMOVE IT AS
    SOON AS A BETTER SOLUTION IS FOUND.
    ☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️☢️

    add_to_sys_module context decorator temporarily adds a module to sys.modules
    """

    def __init__(self, module: ModuleType, id_: str) -> None:
        self._module = module
        self._id_ = id_

    def __enter__(self) -> None:
        if self._id_ in sys.modules:  # pragma: no cover
            raise RuntimeError(f"Attempted to overwrite system module {self._id_}")

        sys.modules[self._id_] = self._module

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        sys.modules.pop(self._id_)


def import_module_from_file(
    module_file_path: Union[str, Path],
    cwd: Optional[Path] = None,
    register_pl_module: bool = True,
) -> ModuleType:
    """Import a python module from a file.

    Args:
        module_file_path (Union[str, Path]): the path to the `.py` module file.
        cwd (Optional[Path], optional): the folder to use for relative module import.
            If None, the file parent folder will be used. Defaults to None.

    Raises:
        ImportError: _description_

    Returns:
        ModuleType: _description_
    """
    from pipelime.cli.utils import PipelimeSymbolsHelper

    module_path = Path(module_file_path)
    if not module_path.is_absolute() and cwd is not None:
        module_path = cwd / module_path

    with add_to_sys_path(str(module_path.parent)):
        m = import_module_from_class_path(module_path.stem, False)
        if register_pl_module:
            PipelimeSymbolsHelper.register_extra_module(module_path.as_posix())
        return m


def import_module_from_class_path(
    module_class_path: str, register_pl_module: bool = True
) -> ModuleType:
    from pipelime.cli.utils import PipelimeSymbolsHelper

    m = importlib.import_module(module_class_path)
    if register_pl_module:
        PipelimeSymbolsHelper.register_extra_module(module_class_path)
    return m


def import_module(
    module_file_or_class_path: str,
    cwd: Optional[Path] = None,
    register_pl_module: bool = True,
) -> ModuleType:
    return (
        import_module_from_file(module_file_or_class_path, cwd, register_pl_module)
        if module_file_or_class_path.endswith(".py")
        else import_module_from_class_path(
            module_file_or_class_path, register_pl_module
        )
    )


def import_symbol(
    symbol_path: str, cwd: Optional[Path] = None, register_pl_module: bool = True
) -> Any:
    """Dynamically imports a given symbol. A symbol can be either:

    - A filesystem path followed by the name of an object to import, like:
        "path/to/my_file.py:MyClass"

    - A python module path, like "module.submodule.MyClass"

    Anything can be imported: modules, classes, functions, etc...

    Args:
        symbol_path (str): The symbol to import
        cwd (Optional[Path], optional): The current working directory to resolve
            relative imports when loading from file. If None, the file parent folder
            will be used. Defaults to None.

    Raises:
        ImportError: If anything goes wrong with the import.

    Returns:
        Any: The dynamically imported object.
    """
    try:
        if ":" in symbol_path:
            # path/to/my_file.py:MyClass
            module_path, _, symbol_name = symbol_path.rpartition(":")
            module_ = import_module_from_file(module_path, cwd, register_pl_module)
            symbol_name = symbol_name.split(".")
        else:
            # package.module.MyClass.MyNestedClass
            module_path, _, symbol_name = symbol_path.rpartition(".")
            symbol_name = [symbol_name]
            module_ = None
            while module_path:
                try:
                    module_ = import_module_from_class_path(
                        module_path, register_pl_module
                    )
                    module_path = None
                except ModuleNotFoundError:
                    # the symbol is nested, so we need to import the parent class path
                    module_path, _, cl_path = module_path.rpartition(".")
                    symbol_name.insert(0, cl_path)

        if module_ is None:
            raise ModuleNotFoundError("Module path not found")

        # import the symbol
        # if nested, we need to import the parent symbols first
        symbol = module_
        for name in symbol_name:
            symbol = getattr(symbol, name)
        return symbol
    except Exception as e:
        raise ImportError(f"Cannot import {symbol_path}") from e
