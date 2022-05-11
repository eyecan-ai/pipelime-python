from pipelime.sequences import SamplesSequence
import typing as t


def _build_op(
    src: t.Union[SamplesSequence, t.Type[SamplesSequence]],
    ops: t.Union[str, t.Mapping[str, t.Any]],
) -> t.Union[SamplesSequence, t.Type[SamplesSequence]]:
    def _op_call(
        seq: t.Union[SamplesSequence, t.Type[SamplesSequence]],
        name: str,
        args: t.Union[t.Mapping[str, t.Any], t.Sequence],
    ) -> SamplesSequence:
        fn = getattr(seq, name)
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
    for op_item in pipe_list if isinstance(pipe_list, t.Sequence) else [pipe_list]:
        source = _build_op(source, op_item)
    return (
        source
        if isinstance(source, SamplesSequence)
        else SamplesSequence.from_list([])  # type: ignore
    )
