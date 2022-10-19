from __future__ import annotations

from abc import ABC, abstractmethod
from math import prod
from statistics import fmean
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple, TypeVar, Union

import numpy as np


T = TypeVar("T", float, Sequence[float], np.ndarray)


class RealFn(ABC):
    def __call__(self, x: T) -> T:
        if isinstance(x, (float, int, bool)):
            return self._call(np.array([float(x)])).tolist()[0]
        elif isinstance(x, Sequence):
            return self._call(np.array(x)).tolist()
        elif isinstance(x, np.ndarray):
            return self._call(x)

    def __repr__(self) -> str:
        return f"f(x) = {self._repr()}"

    @abstractmethod
    def _call(self, x: np.ndarray) -> np.ndarray:
        pass

    @abstractmethod
    def integrate(self) -> RealFn:
        pass

    @abstractmethod
    def invert(self, start: float, stop: float) -> Optional[RealFn]:
        pass

    @abstractmethod
    def shift(self, dx: float, dy: float) -> RealFn:
        pass

    @abstractmethod
    def _repr(self) -> str:
        pass


t_fn_callable = Callable[[np.ndarray], np.ndarray]
t_fn = Union[float, Tuple[float, float], t_fn_callable, RealFn]
t_fn_pw_simple = Sequence[float]
t_fn_pw_ranges = Sequence[Tuple[float, t_fn]]
t_pw = Union[t_fn_pw_simple, t_fn_pw_ranges]
t_pdf = Union[RealFn, t_fn_callable, t_fn_pw_simple, t_fn_pw_ranges, str]


class LinearFn(RealFn):
    def __init__(self, m: float, q: float) -> None:
        super().__init__()
        self.m = m
        self.q = q

    def _call(self, x: np.ndarray) -> np.ndarray:
        return self.m * x + self.q

    def integrate(self) -> RealFn:
        if self.m == 0:
            return LinearFn(self.q, 0)
        return QuadraticFn(self.m / 2, self.q, 0)

    def invert(self, start: float, stop: float) -> Optional[RealFn]:
        if self.m == 0:
            return IdentityFn()
        return LinearFn(1 / self.m, -self.q / self.m)

    def shift(self, dx: float, dy: float) -> LinearFn:
        return LinearFn(self.m, self.q + dy - self.m * dx)

    def _repr(self) -> str:
        return f"{round(self.m, 2)}x + {round(self.q, 2)}"


class IdentityFn(LinearFn):
    def __init__(self) -> None:
        super().__init__(1, 0)

    def _repr(self) -> str:
        return "x"

    def invert(self, start: float, stop: float) -> Optional[RealFn]:
        return IdentityFn()


class ConstantFn(LinearFn):
    def __init__(self, c: float) -> None:
        super().__init__(0, c)

    def _repr(self) -> str:
        return str(round(self.q, 2))

    def integrate(self) -> RealFn:
        return LinearFn(self.q, 0)

    def shift(self, dx: float, dy: float) -> ConstantFn:
        return ConstantFn(self.q + dy)


class QuadraticFn(RealFn):
    def __init__(self, a: float, b: float, c: float) -> None:
        super().__init__()
        self.a = a
        self.b = b
        self.c = c

    def _call(self, x: np.ndarray) -> np.ndarray:
        return self.a * x**2 + self.b * x + self.c

    def integrate(self) -> RealFn:
        raise NotImplementedError("Bruh")

    def invert(self, start: float, stop: float) -> Optional[RealFn]:
        a, b, c, eps = self.a, self.b, self.c, 1e-6
        if a == 0:
            return LinearFn(b, c).invert(start, stop)
        vertex_x = -b / (2 * a)
        if start + eps < vertex_x < stop - eps:
            return None
        sv = -1 if (start + stop) / 2 < vertex_x else 1
        sa = -1 if a < 0 else 1
        p, q = self(np.array([start, stop])).tolist()
        return GenericFn(
            lambda x: (
                -b + sv * sa * np.sqrt(np.clip(b**2 - 4 * a * (c - x), 0, None))
            )
            / (2 * a),
            min(p, q),
            max(p, q),
        )

    def shift(self, dx: float, dy: float) -> QuadraticFn:
        a = self.a
        b = self.b - 2 * a * dx
        c = a * dx**2 - b * dx + self.c + dy
        return QuadraticFn(a, b, c)

    def _repr(self) -> str:
        return f"{round(self.a, 2)}x² + {round(self.b, 2)}x + {round(self.c, 2)}"


class GenericFn(RealFn):
    def __init__(
        self,
        fn: Callable[[np.ndarray], np.ndarray],
        start: float,
        stop: float,
        steps: int = 100,
    ) -> None:
        super().__init__()
        self.fn = fn
        self.start = start
        self.stop = stop
        self.steps = steps

    def _call(self, x: np.ndarray) -> np.ndarray:
        return self.fn(x)

    def _to_piecewise(self) -> PiecewiseFn:
        x = np.linspace(self.start, self.stop, self.steps + 1)
        y = self(x)
        x, y = x.tolist(), y.tolist()
        segments = [(x[i], (y[i], y[i + 1])) for i in range(len(x) - 1)]
        return PiecewiseFn.parse(segments, self.start, self.stop)

    def integrate(self) -> PiecewiseFn:
        return self._to_piecewise().integrate()

    def invert(self, start: float, stop: float) -> Optional[PiecewiseFn]:
        return self._to_piecewise().invert(start, stop)

    def shift(self, dx: float, dy: float) -> GenericFn:
        return GenericFn(lambda x: self(x - dx) + dy, self.start - dx, self.stop - dx)

    def _repr(self) -> str:
        return "code"


class PiecewiseFn(RealFn):
    @classmethod
    def parse_piece(cls, piece: t_fn, start: float, stop: float) -> RealFn:
        if isinstance(piece, RealFn):
            return piece
        elif isinstance(piece, float):
            return ConstantFn(piece)
        elif isinstance(piece, tuple):
            m = (piece[1] - piece[0]) / (stop - start)
            q = piece[0] - m * start
            return LinearFn(m, q)
        elif callable(piece):
            return GenericFn(piece, start, stop)
        else:
            raise TypeError("Piece must be a parsable function")

    @classmethod
    def parse(
        cls,
        fn: t_pw,
        start: Optional[float] = None,
        stop: Optional[float] = None,
    ) -> PiecewiseFn:
        if len(fn) == 0:
            raise ValueError("Piecewise function must have at least one piece")
        if all(isinstance(piece, float) for piece in fn):
            segments = {}
            start_, stop_ = start or 0.0, stop or 1.0
            cur = start_
            step = (stop_ - start_) / len(fn)
            for piece in fn:
                segments[cur] = cls.parse_piece(piece, cur, cur + step)  # type: ignore
                cur += step
            segments[stop_] = ConstantFn(0.0)
        elif all(isinstance(piece, tuple) for piece in fn):
            segments = {}
            keysteps = [x[0] for x in fn]  # type: ignore
            start_, stop_ = start or keysteps[0], stop or keysteps[-1]
            for i in range(len(keysteps)):
                cur, nxt = (
                    keysteps[i],
                    keysteps[i + 1] if i < len(keysteps) - 1 else stop_,
                )
                if cur == nxt:
                    continue
                segments[cur] = cls.parse_piece(fn[i][1], cur, nxt)  # type: ignore
            segments[stop_] = ConstantFn(0.0)
        else:
            raise TypeError("Function must be a list of all floats or all tuples")
        return cls(segments)

    def __init__(self, segments: Mapping[float, RealFn]) -> None:
        self.segments = segments
        self.keysteps = sorted(list(segments.keys()))

    def _call(self, x: np.ndarray) -> np.ndarray:
        results = np.zeros_like(x)
        for i in range(len(self.keysteps) - 1):
            cur, nxt = self.keysteps[i], self.keysteps[i + 1]
            mask = (cur <= x) & (x < nxt)
            results[mask] = self.segments[cur](x[mask])
        return results

    def integrate(self) -> PiecewiseFn:
        segments = {}
        cumsum = 0.0
        for i in range(len(self.keysteps)):
            cur, nxt = (
                self.keysteps[i],
                self.keysteps[i + 1] if i < len(self.keysteps) - 1 else 0,
            )
            integral = self.segments[cur].integrate()
            c0 = cumsum - integral(cur)
            segments[cur] = integral.shift(0, c0)
            cumsum += integral(nxt) - integral(cur)

        segments[float("inf")] = ConstantFn(cumsum)

        return PiecewiseFn(segments)

    def invert(self, start: float, stop: float) -> Optional[PiecewiseFn]:
        segments = {}
        last = -float("inf")
        for i in range(len(self.keysteps) - 1):
            cur, nxt = self.keysteps[i], self.keysteps[i + 1]
            if cur <= start < nxt:
                inverted = self.segments[cur].invert(start, min(nxt, stop))
                if inverted is None:
                    return None
                inverse_start = self.segments[cur](start)
                if inverse_start < last:
                    return None
                last = inverse_start
                segments[last] = inverted
                start = nxt
        return PiecewiseFn(segments)

    def shift(self, dx: float, dy: float) -> RealFn:
        return PiecewiseFn({k - dx: v.shift(dx, dy) for k, v in self.segments.items()})

    def _repr(self) -> str:
        return "piecewise"


def plot(start: float, stop: float, *fn: Optional[RealFn], steps: int = 1000) -> None:
    import matplotlib.pyplot as plt

    start = max(start, -1e3)
    stop = min(stop, 1e3)

    span = stop - start
    input_start = start - span / 5
    input_stop = stop + span / 5
    x = np.linspace(input_start, input_stop, steps)
    y_min, y_max = np.inf, -np.inf
    for f in fn:
        if f is None:
            continue
        y = f(x)
        y_min = min(y_min, np.min(y))
        y_max = max(y_max, np.max(y))

        plt.plot(x, y, linewidth=4)

    y_med = (y_min + y_max) / 2
    y_min = y_med - span / 2
    y_max = y_med + span / 2

    plt.grid()
    plt.axhline(y=0, color="k")
    plt.axvline(x=0, color="k")
    plt.xlim(start, stop)
    plt.show()


class Distribution:
    def __init__(self, fn: RealFn, start: float, stop: float) -> None:
        self.start = start
        self.stop = stop

        primitive = fn.integrate()
        inverse_primitive = primitive.invert(start, stop)
        sum_ = primitive(stop)

        assert inverse_primitive is not None, "The function must be invertible"

        self.pdf = lambda x: fn(np.clip(x, start, stop)) / sum_
        self.cdf = lambda x: primitive(x) / sum_
        self.inverse_cdf = lambda x: inverse_primitive(x * sum_)  # type: ignore

        # plot(start, stop, self.pdf, self.cdf, steps=1000)  # type: ignore

    def sample_n(self, n: int) -> np.ndarray:
        return self.inverse_cdf(np.random.rand(n))


class DistributionFactory:
    @classmethod
    def parse(
        cls, fn: t_pdf, start: Optional[float] = None, stop: Optional[float] = None
    ) -> Distribution:
        if isinstance(fn, str):
            raise NotImplementedError("Parsing strings is not implemented yet")
        if isinstance(fn, RealFn):
            return Distribution(fn, start or 0.0, stop or 1.0)
        elif callable(fn):
            start_, stop_ = start or 0.0, stop or 1.0
            rfn = PiecewiseFn.parse_piece(fn, start_, stop_)  # type: ignore
            return Distribution(rfn, start_, stop_)
        elif isinstance(fn, Sequence):
            rfn = PiecewiseFn.parse(fn, start=start, stop=stop)
            keysteps = sorted(list(rfn.segments.keys()))
            start_, stop_ = keysteps[0], keysteps[-1]
            return Distribution(rfn, start_, stop_)
        else:
            raise ValueError("Invalid distribution")


def _rand(
    start: Optional[float] = None,
    stop: Optional[float] = None,
    n: Union[int, Sequence[int]] = 0,
    pdf: Optional[t_pdf] = None,
    dtype: Optional[Any] = None,
) -> Union[float, Sequence[float]]:
    if pdf is None:
        start_, stop_, n_ = start or 0.0, stop or 1.0, n or 1
        results = np.random.uniform(start_, stop_, n_)
    else:
        distribution = DistributionFactory.parse(pdf, start=start, stop=stop)
        n_ = prod(n) if isinstance(n, Sequence) else (n or 1)
        results = distribution.sample_n(n_)

    if dtype is not None:
        results = results.astype(dtype)

    if n == 0:
        return results.item()
    elif isinstance(n, int):
        return results.tolist()
    else:
        return results.reshape(n)  # type: ignore


def rand(*args, n: int = 0, pdf: Optional[t_pdf] = None) -> Any:
    if len(args) == 0:
        return _rand(n=n, pdf=pdf)
    elif len(args) == 1:
        a = args[0]
        dtype = np.float32 if isinstance(a, float) else np.int32
        return _rand(stop=a, n=n, pdf=pdf, dtype=dtype)
    elif len(args) == 2:
        a, b = args
        dtype = np.float32 if isinstance(a, float) or isinstance(b, float) else np.int32
        return _rand(start=a, stop=b, n=n, pdf=pdf, dtype=dtype)

    raise ValueError("Invalid arguments")


if __name__ == "__main__":
    import matplotlib.pyplot as plt

    n = int(1e5)

    # Uniform random sampling
    # samples = rand(n=n)

    # # Uniform between 1 and 4
    # samples = rand(1.0, 4.0, n=n)

    # Uniform between 0 and 10, but integers
    # samples = rand(0, 10, n=n)

    # pdf = [0.1, 1.0, 2.0, 0.2]
    # samples = rand(0.0, 10.0, n=n, pdf=pdf)

    # pdf = [(0.0, 0.1), (0.5, 1.0), (2.0, 2.0), (2.7, 0.2), (8.0, 1.0)]
    # samples = rand(0.0, 10.0, n=n, pdf=pdf)

    # pdf = [(0.0, 0.1), (2.5, (1.0, 0.4)), (5.0, 2.0), (7.5, (0.0, 0.2))]
    # samples = rand(0.0, 10.0, n=n, pdf=pdf)

    pdf = lambda x: np.exp(-(x**2) / 2) / np.sqrt(2 * np.pi)
    samples = rand(-5.0, 10.0, n=n, pdf=pdf)

    # pdf = [(0.0, (0.0, 0.5)), (0.5, lambda x: 1 / x), (1.0, 0.5)]
    # samples = rand(0.0, 2.0, n=n, pdf=pdf)

    plt.hist(samples, bins=100), plt.show()  # type: ignore
