from typing import Optional


class _StackFrameInfo:
    def __init__(self, stack, pos):
        self._stack = stack
        self._pos = pos
        self._caller = stack[pos]

    @property
    def parent(self):
        return _StackFrameInfo(self._stack, self._pos + 1)

    @property
    def globals(self):
        return self._caller.frame.f_globals

    @property
    def locals(self):
        return self._caller.frame.f_locals

    @property
    def filename(self) -> str:
        return self._caller.filename

    @property
    def lineno(self) -> int:
        return self._caller.lineno

    @property
    def function_name(self) -> str:
        return self._caller.function

    @property
    def module(self) -> str:
        return self._caller.frame.f_globals["__name__"]

    @property
    def package(self) -> str:
        return self.module.partition(".")[0]

    @property
    def docstrings(self) -> Optional[str]:
        import inspect

        return inspect.getdoc(self._caller.frame.f_globals.get(self._caller.function))


def MyCaller(steps_back: int = 0):
    """Get information about your caller.

    Args:
        steps_back (int, optional): use 1 to get the caller of your caller, 2 to get
            the grandparent of your caller, etc. Defaults to 0.
    """
    import inspect

    return _StackFrameInfo(inspect.stack(), 2 + steps_back)
