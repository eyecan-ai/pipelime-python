from __future__ import annotations

import random
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple, Union

import numpy as np

t_callable_fn = Callable[[float], float]
t_seq_fn = Sequence[Union[float, Tuple[float, float]]]
t_map_fn = Mapping[float, Union[float, Tuple[float, float]]]
t_map_fn_s = Mapping[float, Tuple[float, float]]
t_fn = Optional[Union[t_callable_fn, t_seq_fn, t_map_fn]]


class Piecewise:
    N_STEPS = 100

    def __init__(self, segments: t_map_fn_s) -> None:
        self.segments = segments
        self.keysteps = list(sorted(self.segments.keys()))

    @classmethod
    def _map_from_seq(cls, seq: t_seq_fn, start: float, stop: float) -> t_map_fn:
        steps = np.linspace(start, stop, len(seq), endpoint=False).tolist()
        map_fn = {step: value for step, value in zip(steps, seq)}
        map_fn[stop] = 0.0
        return map_fn

    @classmethod
    def from_seq(cls, seq: t_seq_fn, start: float, stop: float) -> Piecewise:
        return cls.from_map(cls._map_from_seq(seq, start, stop), start, stop)

    @classmethod
    def from_map(cls, map_: t_map_fn, start: float, stop: float) -> Piecewise:
        map_ = {k: (v, v) if isinstance(v, float) else v for k, v in map_.items()}
        keysteps = list(sorted(map_.keys()))
        map_[start] = (map_[keysteps[0]][0], map_[keysteps[0]][0])
        map_[stop] = (map_[keysteps[-1]][1], map_[keysteps[-1]][1])
        return Piecewise(map_)

    @classmethod
    def from_derivative(
        cls, derivative: t_callable_fn, start: float, stop: float, steps: int = 100
    ) -> Piecewise:
        cur_sum = 0.0
        segments = {}
        x = np.linspace(start, stop, steps, endpoint=False).tolist()
        dx = (stop - start) / steps
        for i, step in enumerate(x):
            delta = derivative(step) * dx
            segments[step] = (cur_sum, cur_sum + delta)
            cur_sum += delta

        return Piecewise.from_map(segments, start, stop)

    def __call__(self, x: float) -> float:
        if x < self.keysteps[0]:
            return 0.0
        if x >= self.keysteps[-1]:
            return self.segments[self.keysteps[-1]][0]
        for i in range(len(self.keysteps) - 1):
            cur = self.keysteps[i]
            nxt = self.keysteps[i + 1]
            if cur <= x < nxt:
                value = self.segments[cur]
                alpha = (x - cur) / (nxt - cur)
                return value[0] * (1 - alpha) + value[1] * alpha
        return 0.0

    def primitive(self, steps: int = 100) -> Piecewise:
        keysteps = self.keysteps + [self.keysteps[-1]]
        segments = {}
        cur_sum = 0.0
        for i in range(len(keysteps) - 1):
            step = keysteps[i]
            value = self.segments[step]
            if value[0] == value[1]:
                delta = value[0] * (keysteps[i + 1] - step)
                segments[step] = (cur_sum, cur_sum + delta)
                cur_sum += delta
            else:
                dx = (keysteps[i + 1] - step) / steps
                for x in np.linspace(step, keysteps[i + 1], steps):
                    delta = self(x) * dx
                    segments[x] = (cur_sum, cur_sum + delta)
                    cur_sum += delta

        return Piecewise(segments)

    def invert(self) -> Piecewise:
        start, stop = self.keysteps[0], self.keysteps[-1]
        segments = {}
        keysteps = [start] + self.keysteps
        for i in range(len(keysteps) - 1):
            segments[self.segments[keysteps[i]][0]] = (keysteps[i], keysteps[i + 1])
        segments[self.segments[keysteps[-1]][0]] = (keysteps[-1], stop)
        return Piecewise(segments)


class Distribution:
    @classmethod
    def create(cls, pdf: t_fn, start: float, stop: float) -> Distribution:
        if callable(pdf):
            the_fn = lambda x: abs(pdf(x))  # type: ignore
        elif isinstance(pdf, Sequence):
            the_fn = Piecewise.from_seq(pdf, start, stop)
        elif isinstance(pdf, Mapping):
            the_fn = Piecewise.from_map(pdf, start, stop)
        else:
            the_fn = lambda x: 1.0

        return Distribution(the_fn, start, stop)

    def __init__(self, fn: t_callable_fn, start: float, stop: float) -> None:
        self.start = start
        self.stop = stop

        if isinstance(fn, Piecewise):
            primitive = fn.primitive()
        else:
            primitive = Piecewise.from_derivative(fn, start, stop)

        inverse_primitive = primitive.invert()
        sum_ = primitive(stop)

        self.pdf = lambda x: 0.0 if x < start or x >= stop else fn(x) / sum_
        self.cdf = lambda x: primitive(x) / sum_
        self.inverse_cdf = lambda x: inverse_primitive(x * sum_)

    def sample(self) -> float:
        return self.inverse_cdf(random.random())

    def sample_n(self, n: int) -> Sequence[float]:
        return [self.sample() for _ in range(n)]


def sample_from(
    pdf: t_fn, n: int, start: float, stop: float
) -> Union[float, Sequence[float]]:
    if pdf is None:
        results = np.random.uniform(start, stop, n).tolist()

    distribution = Distribution.create(pdf, start, stop)
    results = distribution.sample_n(n)

    return results if n > 0 else results[0]


# random int between a and b
def _rand_int(
    a: int, b: int, n: int = 0, pdf: t_fn = None
) -> Union[int, Sequence[int]]:
    floats = _rand_float(a, b + 1, pdf=pdf)
    if isinstance(floats, float):
        return int(floats)
    return [int(x) for x in floats]


# random int between 0 and a
def _rand_int_single(a: int, n: int = 0, pdf: t_fn = None) -> Union[int, Sequence[int]]:
    return _rand_int(0, a, n=n, pdf=pdf)


# random float between 0 and a
def _rand_float_single(
    a: float, n: int = 0, pdf: t_fn = None
) -> Union[float, Sequence[float]]:
    return _rand_float(0.0, a, n=n, pdf=pdf)


# random float between a and b
def _rand_float(
    a: float, b: float, n: int = 0, pdf: t_fn = None
) -> Union[float, Sequence[float]]:
    return sample_from(pdf, n, a, b)  # type: ignore


def _plot(start: float, stop: float, *fn, steps: int = 1000):
    import matplotlib.pyplot as plt

    x = np.linspace(start, stop, steps, endpoint=False)
    for fn_ in fn:
        plt.plot(x, [fn_(x_) for x_ in x])
    plt.show()


def rand(*args, n: int = 0, pdf: t_fn = None) -> Any:
    if len(args) == 0:
        return _rand_float_single(1.0, n=n, pdf=pdf)
    elif len(args) == 1:
        a = args[0]
        if isinstance(a, int):
            return _rand_int_single(a, n=n, pdf=pdf)
        if isinstance(a, float):
            return _rand_float_single(a, n=n, pdf=pdf)
    elif len(args) == 2:
        a, b = args
        if isinstance(a, int) and isinstance(b, int):
            return _rand_int(a, b, n=n, pdf=pdf)
        if isinstance(a, float) and isinstance(b, float):
            return _rand_float(a, b, n=n, pdf=pdf)

    raise ValueError("Invalid arguments")


if __name__ == "__main__":
    import matplotlib.pyplot as plt

    pdf = [0.1, (1.0, 0.4), 2.0, (0.0, 0.2)]
    samples = rand(0.0, 10.0, n=100000, pdf=pdf)
    plt.hist(samples, bins=100), plt.show()  # type: ignore

    pdf = [0.1, 1.0, 2.0, 0.2]
    samples = rand(0.0, 10.0, n=100000, pdf=pdf)
    plt.hist(samples, bins=100), plt.show()  # type: ignore

    pdf = {0.0: 0.1, 1.0: 1.0, 2.0: 2.0, 3.0: 0.2}
    samples = rand(0.0, 10.0, n=100000, pdf=pdf)
    plt.hist(samples, bins=100), plt.show()  # type: ignore

    # Gaussian pdf
    pdf = lambda x: np.exp(-(x**2) / 2) / np.sqrt(2 * np.pi)
    samples = rand(-5.0, 10.0, n=100000, pdf=pdf)
    plt.hist(samples, bins=100), plt.show()  # type: ignore
