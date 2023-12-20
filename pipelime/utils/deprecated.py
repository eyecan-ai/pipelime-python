import functools
import inspect
import warnings


def deprecated(reason):  # pragma: no cover
    """Decorator to mark classes and functions as deprecated.
    A warning will be emitted when the function is used or the class is created.
    """

    def _helper(wrapped, extra):
        msg = (
            "Call to deprecated "
            f"{'class' if inspect.isclass(wrapped) else 'function'} "
            f"{wrapped.__name__}{extra}"
        )

        def show_warning():
            warnings.simplefilter("always", DeprecationWarning)
            warnings.warn(
                msg,
                category=DeprecationWarning,
                stacklevel=3,
            )
            warnings.simplefilter("default", DeprecationWarning)

        if inspect.isclass(wrapped):
            old_new1 = wrapped.__new__

            def wrapped_new(cls, *args, **kwargs):
                show_warning()
                if old_new1 is object.__new__:
                    return old_new1(cls)  # type: ignore
                return old_new1(cls, *args, **kwargs)

            wrapped.__new__ = staticmethod(functools.wraps(old_new1)(wrapped_new))  # type: ignore

            return wrapped
        else:

            @functools.wraps(wrapped)
            def wrapper(*args, **kwargs):
                show_warning()
                return wrapped(*args, **kwargs)

            return wrapper

    if isinstance(reason, (str, bytes)):
        return functools.partial(_helper, extra=f" ({reason})")
    else:
        return _helper(reason, "")
