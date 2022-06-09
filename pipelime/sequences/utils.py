from __future__ import annotations

import typing as t

from pydantic import BaseModel, Field

from pipelime.sequences import Sample, SamplesSequence


def _build_op(
    src: t.Union[SamplesSequence, t.Type[SamplesSequence]],
    ops: t.Union[str, t.Mapping[str, t.Any]],
) -> t.Union[SamplesSequence, t.Type[SamplesSequence]]:
    def _op_call(
        seq: t.Union[SamplesSequence, t.Type[SamplesSequence]],
        name: str,
        args: t.Union[t.Mapping[str, t.Any], t.Sequence],
    ) -> SamplesSequence:
        if ":" in name:
            from pipelime.choixe.utils.imports import import_module

            module_name, _, op_name = name.rpartition(":")
            import_module(module_name)
        else:
            op_name = name

        fn = getattr(seq, op_name)
        return fn(**args) if isinstance(args, t.Mapping) else fn(*args)

    if isinstance(ops, str):
        return _op_call(src, ops, {})

    for func_name, func_args in ops.items():
        # safe wrapping
        if isinstance(func_args, (str, bytes)) or not isinstance(
            func_args, (t.Sequence, t.Mapping)
        ):
            func_args = [func_args]
        src = _op_call(src, func_name, func_args)

    return src


def build_pipe(
    pipe_list: t.Union[
        str, t.Mapping[str, t.Any], t.Sequence[t.Union[str, t.Mapping[str, t.Any]]]
    ],
    source: t.Union[SamplesSequence, t.Type[SamplesSequence]] = SamplesSequence,
) -> SamplesSequence:
    """Build a pipeline from a list of operations.

    :param pipe_list: a single sequence operator or a mapping or a sequence of mappings
    :type pipe_list: t.Union[ str, t.Mapping[str, t.Any], t.Sequence[t.Union[str, t.Mapping[str, t.Any]]] ]
    :param source: the source symbol to start with, defaults to SamplesSequence
    :type source: t.Union[SamplesSequence, t.Type[SamplesSequence]], optional
    :return: the pipeline
    :rtype: SamplesSequence
    """
    for op_item in pipe_list if isinstance(pipe_list, t.Sequence) else [pipe_list]:
        source = _build_op(source, op_item)
    return (
        source
        if isinstance(source, SamplesSequence)
        else SamplesSequence.from_list([])  # type: ignore
    )


class DataStream(BaseModel, extra="forbid", underscore_attrs_are_private=True):
    """A stream of samples, comprising an input sequence to the data and an output
    pipe to further process the samples when ready.
    """

    input_sequence: SamplesSequence = Field(..., description="Input data sequence.")
    output_pipe: t.Union[
        str, t.Mapping[str, t.Any], t.Sequence[t.Union[str, t.Mapping[str, t.Any]]]
    ] = Field(..., description="Output data pipe.")

    _output_sequence: SamplesSequence
    _curr_keys: t.Optional[t.Sequence[str]]
    _curr_sample: Sample

    def __init__(self, **data):
        from pipelime.stages import StageLambda

        super().__init__(**data)
        sample_mod_seq = self.input_sequence.map(  # type: ignore
            StageLambda(lambda x: self._sample_mod_fn(x))
        )
        self._output_sequence = build_pipe(self.output_pipe, sample_mod_seq)

    @classmethod
    def rw_underfolder(cls, path: str) -> DataStream:
        """Creates a DataStream to read and write samples from the same underfolder."""
        return cls(
            input_sequence=SamplesSequence.from_underfolder(path),  # type: ignore
            output_pipe={"to_underfolder": {"folder": path, "exists_ok": True}},
        )

    def __len__(self) -> int:
        return len(self.input_sequence)

    def __getitem__(self, idx: int) -> Sample:
        x = self.input_sequence[idx]
        for v in x.values():
            v.cache_data = False
        return x

    def process_sample(
        self,
        idx: int,
        new_sample: Sample,
        keys: t.Optional[t.Sequence[str]] = None,
    ):
        """Further process a sample."""
        self._curr_keys = keys
        self._curr_sample = new_sample
        _ = self._output_sequence[idx]

    def _sample_mod_fn(self, *args, **kwargs) -> Sample:
        x = self._curr_sample
        if self._curr_keys:
            x = x.extract_keys(*self._curr_keys)
        return x
