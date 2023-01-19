import pytest
import typing as t
from pydantic import Field
from pipelime.piper import pipelime_command


@pipelime_command
def posonly(a: str, b: int = 42, c: bool = False, /):
    print(a, b, c)


@pipelime_command
def posorkw(a: str, b=42, c: bool = False):
    print(a, b, c)


@pipelime_command
def kwonly(*, a: str, b: int = 42, c: bool = False):
    print(a, b, c)


@pipelime_command
def varpos(*a: str):
    print(a)


@pipelime_command
def varkw(**a: str):
    print(a)


@pipelime_command
def mixed(
    a: str = Field(...),
    b: int = Field(42),
    c: bool = False,
    /,
    d="D",
    e: int = 42,
    f: bool = False,
    *,
    g: str,
    h: int = 42,
    i: bool = False,
):
    print(a, b, c)
    print(d, e, f)
    print(g, h, i)


@pipelime_command
def mixed_var(
    a: str,
    b: int = 42,
    c: bool = False,
    /,
    d: str = "D",
    e: int = 42,
    f: bool = False,
    *args: t.Union[float, int, None],
    g: str,
    h: int = 42,
    i: bool = False,
    **kwargs,
):
    """asdfasdf
    asdfasdfasdf
    asdfasdfasdf
    asdfasdfasdf
    """
    print(a, b, c)
    print(d, e, f)
    print(g, h, i)


class TestCommandDecorator:
    def _error(self, fn, err_cls, *args, **kwargs):
        with pytest.raises(err_cls):
            fn(*args, **kwargs)

    def _type_error(self, fn, *args, **kwargs):
        self._error(fn, TypeError, *args, **kwargs)

    def _validation_error(self, fn, *args, **kwargs):
        self._error(fn, ValueError, *args, **kwargs)

    @pytest.mark.parametrize(
        "cmd", [posonly, posorkw, kwonly, varpos, varkw, mixed, mixed_var]
    )
    def test_help_msg(self, cmd):
        from pipelime.cli.pretty_print import print_models_short_help, print_model_info

        print_models_short_help(cmd, show_class_path=True)
        print_model_info(
            cmd,
            show_class_path=True,
            show_piper_port=True,
            show_description=True,
            recursive=True,
        )

    def test_posonly(self):
        posonly("a")()
        posonly("a", 1)()
        posonly("a", 1, True)()

        self._type_error(posonly)
        self._type_error(posonly, "a", 1, True, "b")
        self._type_error(posonly, a="a")
        self._type_error(posonly, d=None)

    def test_posorkw(self):
        posorkw("a")()
        posorkw("a", 1)()
        posorkw("a", 1, True)()
        posorkw(a="a")()
        posorkw("a", c=True)()

        self._type_error(posorkw)
        self._type_error(posonly, "a", 1, True, "b")
        self._type_error(posonly, d=None)

    def test_kwonly(self):
        kwonly(a="a")()
        kwonly(a="a", b=1)()
        kwonly(a="a", b=1, c=True)()
        kwonly(a="a", c=True)()

        self._validation_error(kwonly)
        self._type_error(kwonly, "a")
        self._type_error(kwonly, "a", 1)
        self._type_error(kwonly, "a", 1, True)
        self._type_error(kwonly, "a", 1, True, "b")
        self._validation_error(kwonly, a="a", d=None)

    def test_varpos(self):
        varpos()()
        varpos("a")()
        varpos("a", 1)()
        varpos("a", 1, True)()

        self._type_error(varpos, a="a")
        self._type_error(varpos, a="a", d=None)

    def test_varkw(self):
        varkw(a="a")()
        varkw(a="a", b=1)()
        varkw(a="a", b=1, c=True)()
        varkw(a="a", c=True)()

        self._validation_error(kwonly)
        self._type_error(varkw, "a")
        self._type_error(varkw, "a", 1)
        self._type_error(varkw, "a", 1, True)
        self._type_error(varkw, "a", 1, True, "b")

    def test_mixed(self):
        mixed("a", g="g")()
        mixed("a", 12, True, "dd", h=17, e=13, g="g")()

        self._type_error(mixed)
        self._validation_error(mixed, "a", 12, True, "dd", h=17, e=13, g="g", i="i")
        self._validation_error(mixed, "a", g="g", m=12)

    def test_mixed_var(self):
        mixed_var("a", g="g")()
        mixed_var("a", 12, True, "dd", 42, False, 45.5, 53.2, g="g", z=42)()

        self._type_error(mixed_var)
        self._validation_error(mixed_var, "a", 12, True, "dd", h=17, e=13, g="g", i="i")
