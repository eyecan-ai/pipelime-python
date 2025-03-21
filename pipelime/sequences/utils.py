from __future__ import annotations

import typing as t
from pathlib import Path

from pydantic.v1 import BaseModel, Field, PrivateAttr

from pipelime.sequences import Sample, SamplesSequence

if t.TYPE_CHECKING:
    from pipelime.commands.interfaces import OutputDatasetInterface


class PipeBuildingError(Exception):
    @classmethod
    def from_exc(cls, message: str, source_exc: Exception):
        return cls(f"{message}\n--> {source_exc}")

    @classmethod
    def from_call(cls, name, sequence, args, source_exc):
        return cls.from_exc(
            f"Error while calling `{name}` on sequence "
            f"`{sequence}` with arguments: {args}\n",
            source_exc,
        )


def _build_op(
    src: t.Union[SamplesSequence, t.Type[SamplesSequence]],
    ops: t.Union[str, t.Mapping[str, t.Any]],
) -> t.Union[SamplesSequence, t.Type[SamplesSequence]]:
    def _op_call(
        seq: t.Union[SamplesSequence, t.Type[SamplesSequence]],
        name: str,
        args: t.Union[t.Mapping[str, t.Any], t.Sequence],
    ) -> SamplesSequence:
        try:
            if ":" in name:
                from pipelime.choixe.utils.imports import import_module

                module_name, _, op_name = name.rpartition(":")
                import_module(module_name)
            else:
                op_name = name

            fn = getattr(seq, op_name)
            try:
                return fn(**args) if isinstance(args, t.Mapping) else fn(*args)
            except TypeError:
                # try to call without expanding args
                return fn(args)
        except Exception as e:
            raise PipeBuildingError.from_call(name, seq, args, e)

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

    Args:
        pipe_list (Union[str,
            Mapping[str, Any], Sequence[Union[str, Mapping[str, Any]]]]): a single
            sequence operator or a mapping or a sequence of mappings
        source (Union[SamplesSequence, Type[SamplesSequence]], optional): the source
            symbol to start with  (Default to SamplesSequence)

    Returns:
        SamplesSequence: the pipeline
    """
    for op_item in (
        pipe_list
        if isinstance(pipe_list, t.Sequence) and not isinstance(pipe_list, str)
        else [pipe_list]
    ):
        source = _build_op(source, op_item)
    if not isinstance(source, SamplesSequence):
        raise PipeBuildingError(
            f"Pipe `{repr(pipe_list)}` does not return a samples sequence instance."
        )
    return source


class DataStream(
    t.Sequence[Sample],
    BaseModel,
    extra="forbid",
    copy_on_model_validation="none",
    underscore_attrs_are_private=True,
):
    """A stream of samples, comprising an input sequence to the data and an output
    pipe to further process the samples when ready.
    """

    class _SampleGen:
        _curr_keys: t.Optional[t.Sequence[str]]
        _curr_sample: Sample

        def __call__(self, _: int) -> Sample:
            x = self._curr_sample
            if self._curr_keys:
                x = x.extract_keys(*self._curr_keys)
            return x

    input_sequence: t.Optional[SamplesSequence] = Field(
        None, description="Input data sequence."
    )
    output_pipe: t.Union[
        str, t.Mapping[str, t.Any], t.Sequence[t.Union[str, t.Mapping[str, t.Any]]]
    ] = Field(..., description="Output data pipe.")

    _output_sequence: SamplesSequence
    _sample_gen: _SampleGen = PrivateAttr(default_factory=_SampleGen)

    def __init__(self, **data):
        super().__init__(**data)
        self._output_sequence = build_pipe(
            self.output_pipe,
            SamplesSequence.from_callable(  # type: ignore
                generator_fn=self._sample_gen, length=1
            ),
        )

    @classmethod
    def from_output_dataset(
        cls,
        output_interface: "OutputDatasetInterface",
        bidirectional: bool = False,
    ) -> DataStream:
        """Creates a DataStream from an input and an output interface.

        Args:
            output_interface (OutputDatasetInterface): The output interface to use.
            bidirectional (bool, optional): Whether the stream should be readable too.
                Defaults to False.

        Returns:
            DataStream: The created DataStream.
        """
        return cls(
            input_sequence=(
                output_interface.as_input().create_reader() if bidirectional else None
            ),
            output_pipe=output_interface.as_pipe(),
        )

    @classmethod
    def read_write_underfolder(
        cls,
        path: t.Union[str, Path],
        must_exists: bool = True,
        zfill: t.Optional[int] = None,
    ) -> DataStream:
        """Creates a DataStream to read and write samples from the same underfolder."""
        seq: SamplesSequence = SamplesSequence.from_underfolder(
            path, must_exist=must_exists, watch=True  # type: ignore
        )
        return cls(
            input_sequence=seq,  # type: ignore
            output_pipe={
                "to_underfolder": {
                    "folder": str(path),
                    "exists_ok": True,
                    "zfill": (
                        seq.best_zfill() if zfill is None else zfill
                    ),  # to be consistent if samples are added
                }
            },
        )

    @classmethod
    def create_new_underfolder(
        cls, path: t.Union[str, Path], zfill: int = 0
    ) -> DataStream:
        """Creates a DataStream to write samples to a new underfolder dataset."""
        seq: SamplesSequence = SamplesSequence.from_underfolder(
            path, must_exist=False, watch=True  # type: ignore
        )
        return cls(
            input_sequence=seq,  # type: ignore
            output_pipe={
                "to_underfolder": {
                    "folder": str(path),
                    "exists_ok": False,
                    "zfill": zfill,
                }
            },
        )

    @classmethod
    def create_output_stream(
        cls, path: t.Union[str, Path], zfill: int = 0
    ) -> DataStream:
        """Creates a DataStream to write samples to a new underfolder dataset
        or update an existing one. NB: Samples cannot be read from this stream."""
        return cls(
            input_sequence=None,
            output_pipe={
                "to_underfolder": {
                    "folder": str(path),
                    "exists_ok": True,
                    "zfill": zfill,
                }
            },
        )

    def __len__(self) -> int:
        return 0 if self.input_sequence is None else len(self.input_sequence)

    def __getitem__(self, idx: int) -> Sample:
        if self.input_sequence is None:
            raise IndexError("No input sequence.")

        x = self.input_sequence[idx]
        for v in x.values():
            v.cache_data = False  # always watch for changes
        return x

    def get_input(self, idx: int) -> Sample:
        return self[idx]

    def set_output(
        self,
        idx: int,
        sample: Sample,
        keys: t.Optional[t.Sequence[str]] = None,
    ):
        self._sample_gen._curr_keys = keys
        self._sample_gen._curr_sample = sample
        _ = self._output_sequence[idx]
