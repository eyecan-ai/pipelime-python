import importlib
import importlib.util
import sys
import threading
import weakref
from contextlib import ContextDecorator
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Optional, Union

_imp_lock = threading.Lock()
_module_locks: Dict[str, "weakref.ReferenceType[threading.Lock]"] = {}


# We follow the importlib implementation about module locking
# to lock when importing modules from python code strings
# NB: no need to check for deadlock, as it is used only for anonymous modules
def _get_module_lock(name):
    """Get or create the module lock for a given module name."""
    # Acquire/release internally the global import lock to protect _module_locks.
    with _imp_lock:
        try:
            lock = _module_locks[name]()
        except KeyError:
            lock = None

        if lock is None:
            lock = threading.Lock()

            def cb(ref, name=name):
                with _imp_lock:
                    # bpo-31070: Check if another thread created a new lock
                    # after the previous lock was destroyed
                    # but before the weakref callback was called.
                    if _module_locks.get(name) is ref:
                        del _module_locks[name]

            _module_locks[name] = weakref.ref(lock, cb)

        return lock


class _ModuleLockManager:
    def __init__(self, name):
        self._name = name
        self._lock = None

    def __enter__(self):
        self._lock = _get_module_lock(self._name)
        self._lock.acquire()

    def __exit__(self, *args, **kwargs):
        self._lock.release()  # type: ignore


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

    if not module_path.exists():
        raise ModuleNotFoundError(f"Module not found: {module_file_path}")

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


def import_module_from_code(
    module_code: str, register_pl_module: bool = True
) -> ModuleType:
    import hashlib

    from pipelime.cli.utils import PipelimeSymbolsHelper

    hash_fn = hashlib.blake2b()
    hash_fn.update(module_code.encode("utf-8"))
    name = hash_fn.hexdigest()

    with _ModuleLockManager(name):
        # check if the module is already imported
        try:
            spec = importlib.util.find_spec(name)
        except Exception:
            spec = None

        if spec is not None:
            module = importlib.import_module(name)
        else:
            # create a new module from code
            spec = importlib.util.spec_from_loader(name, loader=None)
            if spec is None:
                raise ImportError(f"Cannot create spec for module `{module_code}`")
            module = importlib.util.module_from_spec(spec)

            # compile the code and put everything in the new module
            exec(module_code, module.__dict__)

            # add the module to sys.modules
            sys.modules[name] = module

        if register_pl_module:
            PipelimeSymbolsHelper.register_extra_module(module_code)
        return module


def import_module(
    module_file_or_class_path_or_code: str,
    cwd: Optional[Path] = None,
    register_pl_module: bool = True,
) -> ModuleType:
    err_msgs = []

    if module_file_or_class_path_or_code.endswith(".py"):
        try:
            return import_module_from_file(
                module_file_or_class_path_or_code, cwd, register_pl_module
            )
        except Exception as e:
            err_msgs.append(str(e))
    try:
        return import_module_from_class_path(
            module_file_or_class_path_or_code, register_pl_module
        )
    except Exception as e:
        err_msgs.append(str(e))

    try:
        return import_module_from_code(
            module_file_or_class_path_or_code, register_pl_module
        )
    except Exception as e:
        err_msgs.append(str(e))

    raise ImportError(
        "Cannot import:\n"
        f"  `{module_file_or_class_path_or_code}`\n"
        "Errors:\n"
        f"  from file: {err_msgs[0]}\n"
        f"  from classpath: {err_msgs[1]}\n"
        f"  from code: {err_msgs[2]}"
    )


def import_symbol(
    symbol_definition: str, cwd: Optional[Path] = None, register_pl_module: bool = True
) -> Any:
    """Dynamically imports a given symbol. A symbol can be either:

    - A filesystem path followed by the name of an object to import, like
        "path/to/my_file.py:MyClass"
    - A python module path, like "module.submodule.MyClass"
    - A lambda function, like "lambda x: x + 1"
    - A symbol name followed by 3 colons and the definition of the symbol, like
        "Foo.Bar:::class Foo:\n  class Bar:\n    pass"

    Anything can be imported: modules, classes, functions, etc...

    Args:
        symbol_definition (str): The symbol to import
        cwd (Optional[Path], optional): The current working directory to resolve
            relative imports when loading from file. If None, the file parent folder
            will be used. Defaults to None.

    Raises:
        ImportError: If anything goes wrong with the import.

    Returns:
        Any: The dynamically imported object.
    """
    import ast

    try:
        # I need this for lambda imports
        force_symbol_name = None

        try:
            parsed = ast.parse(symbol_definition, mode="eval")
        except SyntaxError:
            symb, _, code = symbol_definition.partition(":::")
            if symb and code:
                # Foo.Bar:::class Foo:\n  class Bar:\n    pass
                module_ = import_module_from_code(code, register_pl_module)
                symbol_name = symb.split(".")
            else:
                # path/to/my_file.py:MyClass
                module_path, _, symbol_name = symbol_definition.rpartition(":")
                module_ = import_module_from_file(module_path, cwd, register_pl_module)
                symbol_name = symbol_name.split(".")
        else:
            # parse succeeded
            if isinstance(parsed.body, ast.Lambda):
                # lambda x: x + 1
                symbol_definition = "lambda_fn = " + symbol_definition
                module_ = import_module_from_code(symbol_definition, register_pl_module)
                symbol_name = ["lambda_fn"]
                force_symbol_name = "lambda_fn"
            elif isinstance(parsed.body, ast.Attribute) and isinstance(
                parsed.body.ctx, ast.Load
            ):
                # package.module.MyClass.MyNestedClass
                module_path, _, symbol_name = symbol_definition.rpartition(".")
                symbol_name = [symbol_name]
                module_ = None
                while module_path:
                    try:
                        module_ = import_module_from_class_path(
                            module_path, register_pl_module
                        )
                        module_path = None
                    except ModuleNotFoundError:
                        # Nested symbol, so we need to import the parent class path
                        module_path, _, cl_path = module_path.rpartition(".")
                        symbol_name.insert(0, cl_path)

                if module_ is None:
                    raise ModuleNotFoundError("Module not found")
            else:
                raise ImportError("unknow definition")

        if not symbol_name:
            raise ImportError("No symbol name found")

        # import the symbol
        # if nested, we need to import the parent symbols first
        symbol = module_
        for name in symbol_name:
            symbol = getattr(symbol, name)

        if force_symbol_name:
            symbol.__name__ = force_symbol_name
            symbol.__qualname__ = force_symbol_name  # type: ignore
        return symbol
    except Exception as e:
        raise ImportError(f"Cannot import `{symbol_definition}`") from e
