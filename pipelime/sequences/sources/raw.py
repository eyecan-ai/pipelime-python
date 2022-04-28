import typing as t

import pipelime.sequences.samples_sequence as pls


@pls.as_samples_sequence_functional("from_list", is_static=True)
class SamplesList(pls.SamplesSequence):
    """A SamplesSequence from a list of Samples. Usage::

        sseq = SamplesSequence.from_list([...])

    :param samples: the source sequence of samples.
    :type samples: t.Sequence[pls.Sample]
    """

    def __init__(self, samples: t.Sequence[pls.Sample]):
        super().__init__()
        self._samples = samples

    @property
    def samples(self) -> t.Sequence[pls.Sample]:
        return self._samples

    def size(self) -> int:
        return len(self._samples)

    def get_sample(self, idx: int) -> pls.Sample:
        return self._samples[idx]
