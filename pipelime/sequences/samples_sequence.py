from __future__ import annotations
from abc import abstractmethod
import itertools
import typing as t

from pipelime.sequences.sample import Sample


class SamplesSequence(t.Sequence[Sample]):
    """A generic sequence of samples. Subclasses should implement `size(self) -> int`
    and `get_sample(self, idx: int) -> Sample`.

    The list of all available pipes and sources can be retrieved through
    `SamplesSequence.pipes` and `SamplesSequence.sources`. Descriptive help
    messages are provided for each method, eg, try `help(SamplesSequence.map)`.
    """

    pipes: t.List[str] = []
    sources: t.List[str] = []

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
            self.slice(idx.start, idx.stop, idx.step)  # type: ignore
            if isinstance(idx, slice)
            else self.get_sample(idx)
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

    def __add__(self, other: "SamplesSequence") -> SamplesSequence:
        return self.cat(other)  # type: ignore


def as_samples_sequence_functional(fn_name: str, is_static: bool = False):
    """A decorator registering a SamplesSequence subclass as functional attribute.

    :param fn_name: the name of the function that we are going to add.
    :type fn_name: str
    """

    def _wrapper(cls):
        import inspect

        docstr = inspect.getdoc(cls)
        if docstr:
            docstr = docstr.replace("\n", "\n    ")

        sig = inspect.signature(cls.__init__)
        prms_list = [i[1] for i in sig.parameters.items()]

        if is_static:
            prms_list = prms_list[1:]
        else:
            prms_list = [prms_list[0]] + prms_list[2:]

        # NB: including annotations would be great, but you'd need to import here
        # all kind of packages used by the subclass we are wrapping.
        prm_names = [p.name for p in prms_list]
        prm_defaults = [
            "" if p.default is inspect.Parameter.empty else f"={p.default}"
            for p in prms_list
        ]
        prm_def_str = ", ".join(
            f"{pname}{pdef}" for pname, pdef in zip(prm_names, prm_defaults)
        )

        fn_str = (
            "def _{0}({1}):\n".format(fn_name, prm_def_str)
            + ("    '''{0}\n    '''\n".format(docstr) if docstr else "")
            + "    from {0} import {1}\n".format(cls.__module__, cls.__name__)
            + "    return {0}({1})\n".format(
                cls.__name__,
                ", ".join(sig.replace(parameters=prms_list).parameters.keys()),
            )
        )

        local_scope = {}
        exec(fn_str, local_scope)

        fn_helper = local_scope[f"_{fn_name}"]
        if is_static:
            fn_helper = staticmethod(fn_helper)

        setattr(SamplesSequence, fn_name, fn_helper)

        if is_static:
            SamplesSequence.sources.append(fn_name)
        else:
            SamplesSequence.pipes.append(fn_name)

        return cls

    return _wrapper
