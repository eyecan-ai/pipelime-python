from __future__ import annotations
from abc import abstractmethod
import itertools
import typing as t
import pydantic as pyd

from pipelime.sequences.sample import Sample


class SamplesSequenceBase(t.Sequence[Sample]):
    pipes: t.Dict[str, t.Dict[str, t.Any]] = {}
    sources: t.Dict[str, t.Dict[str, t.Any]] = {}

    @abstractmethod
    def size(self) -> int:
        pass

    @abstractmethod
    def get_sample(self, idx: int) -> Sample:
        pass

    def __len__(self) -> int:
        return self.size()

    def __getitem__(self, idx: t.Union[int, slice]) -> Sample:
        return (
            self.slice(start=idx.start, stop=idx.stop, step=idx.step)  # type: ignore
            if isinstance(idx, slice)
            else self.get_sample(idx if idx >= 0 else len(self) + idx)
        )

    def is_normalized(self, max_items=-1) -> bool:
        """Checks if all samples have the same keys.

        :param max_items: limits to the first `max_items`, defaults to -1
        :type max_items: int, optional
        :return: True if all samples have the same keys
        :rtype: bool
        """
        max_items = len(self) if max_items < 0 else min(max_items, len(self))
        if max_items < 2:
            return True
        it = itertools.islice(self, max_items)
        key_ref = set(next(it).keys())
        for s in it:
            if key_ref != set(s.keys()):
                return False
        return True

    def best_zfill(self) -> int:
        """Computes the best zfill for integer indexing.

        :return: zfill values (maximum number of digits based on current size)
        :rtype: int
        """
        return len(str(len(self)))

    def __add__(self, other: SamplesSequence) -> SamplesSequence:
        return self.cat(other)  # type: ignore


class SamplesSequence(SamplesSequenceBase, pyd.BaseModel):
    """A generic sequence of samples. Subclasses should implement `size(self) -> int`
    and `get_sample(self, idx: int) -> Sample`.

    The list of all available pipes and sources, along with respective schemas, can be
    retrieved through `SamplesSequence.pipes` and `SamplesSequence.sources`. Also,
    descriptive help messages are provided for each method, eg, try
    `help(SamplesSequence.map)`.

    NB: when defining a pipe, the `source` sample sequence must be bound to a pydantic
    Field with `pipe_source=True`.
    """

    # the function attribute generating this instance
    _fn_name: str = pyd.PrivateAttr("")

    def pipe(self, recursive: bool) -> t.List[t.Dict[str, t.Any]]:
        source_list = []
        arg_dict = {}
        for field_name, model_field in self.__fields__.items():
            field_value = getattr(self, field_name)
            if model_field.field_info.extra.get("pipe_source", False):
                if not isinstance(field_value, SamplesSequence):
                    raise ValueError(
                        f"{field_name} is tagged as `pipe_source`, "
                        "so it must be a SamplesSequence instance."
                    )
                source_list = field_value.pipe(recursive)
            else:
                # NB: do not unfold sub-pydantic models, since it may not be
                # straightforward to de-serialize them when subclasses are used
                if recursive and isinstance(field_value, SamplesSequence):
                    field_value = field_value.pipe(recursive)
                arg_dict[field_name] = field_value
        return source_list + [{self._fn_name: arg_dict}]


def as_samples_sequence_functional(fn_name: str, is_static: bool = False):  # noqa: C901
    """A decorator registering a SamplesSequence subclass as functional attribute.

    NB: when defining a pipe, the `source` sample sequence must be bound to a pydantic
    Field with `pipe_source=True`.

    :param fn_name: the name of the function that we are going to add.
    :type fn_name: str
    """

    if fn_name in SamplesSequence.sources or fn_name in SamplesSequence.pipes:
        raise ValueError(f"Function {fn_name} has been already registered.")

    def _wrapper(cls):
        import inspect
        import warnings

        docstr = inspect.getdoc(cls)
        if docstr:
            docstr = docstr.replace("\n", "\n    ")

        # NB: cls is a pydantic model!
        # The signature of the model class does not include the `self` argument
        sig = inspect.signature(cls)
        prms_list = [p for p in sig.parameters.values()]

        def _get_pyd_field_info(model, field_name):
            mfield = None
            if field_name in model.__fields__:
                mfield = model.__fields__[field_name]
            else:
                for mf in model.__fields__.values():
                    if field_name == mf.alias:
                        mfield = mf
                        break
            if mfield is None:
                raise NameError(f"Field {field_name} not found in {model.__name__}.")
            return mfield.field_info

        if is_static:
            fn_def_self, fn_call_self = "", ""
        else:
            # remove the `source` parameter (which will be set as `self`)
            prms_source_name = None
            for i, p in enumerate(prms_list[:]):  # NB: we need a copy!
                if _get_pyd_field_info(cls, p.name).extra.get("pipe_source", False):
                    if prms_source_name is not None:
                        raise ValueError(
                            "More than one field has "
                            f"`pipe_source=True` in {cls.__name__}."
                        )
                    prms_source_name = prms_list.pop(i).name

            if prms_source_name is None:
                raise ValueError(
                    f"{cls.__name__} is tagged as `piped`, "
                    "but no field has `pipe_source=True`."
                )

            fn_def_self = "self, "
            fn_call_self = prms_source_name + "=self, "

        # NB: including annotations would be great, but you'd need to import here
        # all kind of packages used by the subclass we are wrapping.
        prm_def_str = []
        slash_added = True
        star_added = False
        import_code_str = ""
        for prm in prms_list:
            if prm.kind == inspect.Parameter.POSITIONAL_ONLY:
                slash_added = False
            if not slash_added and prm.kind != inspect.Parameter.POSITIONAL_ONLY:
                prm_def_str.append("/")
                slash_added = True
            if not star_added and prm.kind == inspect.Parameter.KEYWORD_ONLY:
                prm_def_str.append("*")
                star_added = True

            if prm.default is inspect.Parameter.empty:
                pdef = ""
            elif inspect.isclass(prm.default):
                pdef = f"={prm.default.__module__}.{prm.default.__name__}"
                import_code_str += f"import {prm.default.__module__}\n"
            elif isinstance(prm.default, (str, bytes)):
                pdef = f"='{prm.default}'"
            else:
                pdef = f"={prm.default}"

            prm_def_str.append(f"{prm.name}{pdef}")
        prm_def_str = ", ".join(prm_def_str)

        # call by name
        call_by_name_str = ", ".join(
            [f"{k}={k}" for k in sig.replace(parameters=prms_list).parameters.keys()]
        )

        # The following function is generated and evaluated at runtime:
        # """
        # def _<name>([self, ]prm0, prm1=val1):
        #     '''<docstring>'''
        #     from <module> import <subclass>
        #     seq = <subclass>([self, ]prm0=prm0, prm1=prm1)
        #     seq._fn_name = '<fn_name>'
        #     return seq
        # """
        fn_str = (
            import_code_str
            + "def _{0}({1}{2}):\n".format(fn_name, fn_def_self, prm_def_str)
            + ("    '''{0}\n    '''\n".format(docstr) if docstr else "")
            + "    from {0} import {1}\n".format(cls.__module__, cls.__name__)
            + "    seq = {0}({1}{2})\n".format(
                cls.__name__, fn_call_self, call_by_name_str
            )
            + "    seq._fn_name = '{0}'\n".format(fn_name)
            + "    return seq\n"
        )

        local_scope = {}
        exec(fn_str, local_scope)

        fn_helper = local_scope[f"_{fn_name}"]
        if is_static:
            fn_helper = staticmethod(fn_helper)

        setattr(SamplesSequence, fn_name, fn_helper)

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", ".*was excluded from schema.*")
            if is_static:
                SamplesSequence.sources[fn_name] = cls.schema()
            else:
                SamplesSequence.pipes[fn_name] = cls.schema()

        return cls

    return _wrapper


def source_sequence(fn_name: str):
    return as_samples_sequence_functional(fn_name, is_static=True)


def piped_sequence(fn_name: str):
    return as_samples_sequence_functional(fn_name, is_static=False)
