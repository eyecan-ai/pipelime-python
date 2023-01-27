import pytest
import typing as t
from pydantic import Field
from pipelime.piper import command, PipelimeCommand


@command
def posonly(a: str, b: int = 42, c: bool = False, /):
    print(a, b, c)


@command
def posorkw(a: str, b=42, c: bool = False):
    print(a, b, c)


@command
def kwonly(*, a: str, b: int = 42, c: bool = False):
    print(a, b, c)


@command
def varpos(*a: str):
    print(a)


@command
def varkw(**a: str):
    print(a)


@command
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


@command
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
    **kwargs: int,
):
    """asdfasdf
    asdfasdfasdf
    asdfasdfasdf
    asdfasdfasdf
    """
    print(a, b, c)
    print(d, e, f)
    print(g, h, i)


class MyType:
    pass


@command(title="add-up-these", arbitrary_types_allowed=True)
def my_addup_func(a: int, b: int, c: t.Optional[MyType] = None):
    """This is a function that adds two numbers"""
    print(a + b)


class TestCommandDecorator:
    def _error(self, fn, err_cls, *args, **kwargs):
        with pytest.raises(err_cls):
            fn(*args, **kwargs)

    def _type_error(self, fn, *args, **kwargs):
        self._error(fn, TypeError, *args, **kwargs)

    def _validation_error(self, fn, *args, **kwargs):
        self._error(fn, ValueError, *args, **kwargs)

    @pytest.mark.parametrize(
        "cmd",
        [posonly, posorkw, kwonly, varpos, varkw, mixed, mixed_var, my_addup_func],
    )
    def test_is_command(self, cmd):
        from pipelime.cli.pretty_print import print_models_short_help, print_model_info
        from pipelime.cli.utils import PipelimeSymbolsHelper, get_pipelime_command_cls

        assert issubclass(cmd, PipelimeCommand)

        PipelimeSymbolsHelper.set_extra_modules(
            ["tests.pipelime.piper.test_command_decorator"]
        )
        other_cmd = get_pipelime_command_cls(cmd.command_title(), interactive=False)
        assert other_cmd is cmd

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

        self._validation_error(posonly)
        self._type_error(posonly, "a", 1, True, "b")
        self._type_error(posonly, a="a")
        self._validation_error(posonly, d=None)

    def test_posorkw(self):
        posorkw("a")()
        posorkw("a", 1)()
        posorkw("a", 1, True)()
        posorkw(a="a")()
        posorkw("a", c=True)()

        self._validation_error(posorkw)
        self._type_error(posorkw, "a", 1, True, "b")
        self._validation_error(posorkw, d=None)

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
        self._validation_error(varpos, d=None)

    def test_varkw(self):
        varkw()()
        varkw(a="a")()
        varkw(a="a", b=1)()
        varkw(a="a", b=1, c=True)()
        varkw(a="a", c=True)()

        self._type_error(varkw, "a")
        self._type_error(varkw, "a", 1)
        self._type_error(varkw, "a", 1, True)
        self._type_error(varkw, "a", 1, True, "b")

    def test_mixed(self):
        mixed("a", g="g")()
        mixed("a", 12, True, "dd", h=17, e=13, g="g")()

        self._validation_error(mixed)
        self._validation_error(mixed, "a", 12, True, "dd", h=17, e=13, g="g", i="i")
        self._validation_error(mixed, "a", g="g", m=12)

    def test_mixed_var(self):
        mixed_var("a", g="g")()
        mixed_var("a", 12, True, "dd", 42, False, 45.5, 53.2, g="g", z=42)()

        self._validation_error(mixed_var)
        self._validation_error(mixed_var, "a", 12, True, "dd", h=17, e=13, g="g", i="i")
        self._type_error(mixed_var, "a", 12, True, b=13, g="g")
        self._type_error(mixed_var, "a", 12, True, "d", d="dd", g="g")
        self._validation_error(mixed_var, "a", g="g", z="zz")

    def test_my_addup_func(self):
        my_addup_func(1, 2)()
        my_addup_func(1, 2, MyType())()
        my_addup_func(c=MyType(), b=2, a=1)()

        self._validation_error(my_addup_func)
        self._type_error(my_addup_func, 1, 2, b=3)
        self._validation_error(my_addup_func, None, None, None)
        self._validation_error(my_addup_func, 1, 2, 3)
