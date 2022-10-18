from __future__ import annotations

from abc import ABC, abstractmethod
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


t_piece_fn = Callable[[np.ndarray], np.ndarray]
t_piece = Union[float, Tuple[float, float], t_piece_fn, RealFn]
t_piecewise = Union[Sequence[t_piece], Mapping[float, t_piece]]
t_pdf = Union[RealFn, t_piece_fn, Sequence[t_piece], Mapping[float, t_piece]]


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
        print(vertex_x, start, stop)
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
        segments = {x[i]: (y[i], y[i + 1]) for i in range(len(x) - 1)}
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
    def parse_piece(cls, piece: t_piece, start: float, stop: float) -> RealFn:
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
        fn: t_piecewise,
        start: Optional[float] = None,
        stop: Optional[float] = None,
    ) -> PiecewiseFn:
        if isinstance(fn, Sequence):
            segments = {}
            start_, stop_ = start or 0.0, stop or 1.0
            cur = start_
            step = (stop_ - start_) / len(fn)
            for piece in fn:
                segments[cur] = cls.parse_piece(piece, cur, cur + step)
                cur += step
            segments[stop_] = ConstantFn(0.0)
        elif isinstance(fn, Mapping):
            segments = {}
            keysteps = sorted(list(fn.keys()))
            start_, stop_ = start or keysteps[0], stop or keysteps[-1]
            for i in range(len(keysteps)):
                cur, nxt = (
                    keysteps[i],
                    keysteps[i + 1] if i < len(keysteps) - 1 else stop_,
                )
                segments[cur] = cls.parse_piece(fn[cur], cur, nxt)
            segments[stop_] = ConstantFn(0.0)
        else:
            raise TypeError("Invalid function")
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
    # plt.axis("equal")
    plt.xlim(start, stop)
    plt.show()


class Distribution:
    def __init__(self, fn: RealFn, start: float, stop: float) -> None:
        self.start = start
        self.stop = stop

        primitive = fn.integrate()
        inverse_primitive = primitive.invert(start, stop)
        sum_ = primitive(stop)

        self.pdf = lambda x: fn(np.clip(x, start, stop)) / sum_
        self.cdf = lambda x: primitive(x) / sum_
        self.inverse_cdf = lambda x: inverse_primitive(x * sum_)  # type: ignore

        plot(start, stop, self.pdf, self.cdf, steps=1000)  # type: ignore

    def sample_n(self, n: int) -> np.ndarray:
        return self.inverse_cdf(np.random.rand(n))

    @classmethod
    def parse(
        cls, fn: t_pdf, start: Optional[float] = None, stop: Optional[float] = None
    ) -> Distribution:
        if isinstance(fn, RealFn):
            return cls(fn, start or -float("inf"), stop or float("inf"))
        elif callable(fn):
            start_, stop_ = start or -float("inf"), stop or float("inf")
            rfn = PiecewiseFn.parse_piece(fn, start_, stop_)  # type: ignore
            return cls(rfn, start_, stop_)
        elif isinstance(fn, (Sequence, Mapping)):
            rfn = PiecewiseFn.parse(fn)
            keysteps = sorted(list(rfn.segments.keys()))
            start_, stop_ = keysteps[0], keysteps[-1]
            return cls(rfn, start_, stop_)
        else:
            raise ValueError("Invalid distribution")


def _rand_floats(
    start: Optional[float] = None,
    stop: Optional[float] = None,
    n: int = 0,
    pdf: Optional[t_pdf] = None,
) -> Union[float, Sequence[float]]:
    if pdf is None:
        start_, stop_, n_ = start or 0.0, stop or 1.0, n or 1
        results = np.random.uniform(start_, stop_, n_).tolist()
    else:
        distribution = Distribution.parse(pdf, start=start, stop=stop)
        n_ = n or 1
        results = distribution.sample_n(n_).tolist()

    return results if n > 0 else results[0]


def _rand_ints(
    start: Optional[int] = None,
    stop: Optional[int] = None,
    n: int = 0,
    pdf: Optional[t_pdf] = None,
) -> Union[int, Sequence[int]]:
    floats = _rand_floats(start=start, stop=stop, n=n, pdf=pdf)
    if isinstance(floats, float):
        return int(round(floats))
    return [int(round(x)) for x in floats]


def rand(*args, n: int = 0, pdf: Optional[t_pdf] = None) -> Any:
    if len(args) == 0:
        return _rand_floats(n=n, pdf=pdf)
    elif len(args) == 1:
        a = args[0]
        if isinstance(a, int):
            return _rand_ints(stop=a, n=n, pdf=pdf)
        if isinstance(a, float):
            return _rand_floats(stop=a, n=n, pdf=pdf)
    elif len(args) == 2:
        a, b = args
        if isinstance(a, int) and isinstance(b, int):
            return _rand_ints(start=a, stop=b, n=n, pdf=pdf)
        if isinstance(a, float) and isinstance(b, float):
            return _rand_floats(start=a, stop=b, n=n, pdf=pdf)

    raise ValueError("Invalid arguments")


if __name__ == "__main__":
    import matplotlib.pyplot as plt

    n = int(1e7)

    pdf = [0.1, (1.0, 0.4), 2.0, (0.0, 0.2)]
    samples = rand(0.0, 10.0, n=n, pdf=pdf)
    plt.hist(samples, bins=100), plt.show()  # type: ignore

    pdf = [0.1, 1.0, 2.0, 0.2]
    samples = rand(0.0, 10.0, n=n, pdf=pdf)
    plt.hist(samples, bins=100), plt.show()  # type: ignore

    pdf = {0.0: 0.1, 1.0: 1.0, 2.0: 2.0, 3.0: 0.2, 15.0: 120.0}
    samples = rand(0.0, 10.0, n=n, pdf=pdf)
    plt.hist(samples, bins=100), plt.show()  # type: ignore

    # Gaussian pdf
    pdf = lambda x: np.exp(-(x**2) / 2) / np.sqrt(2 * np.pi)
    samples = rand(-5.0, 10.0, n=n, pdf=pdf)
    plt.hist(samples, bins=100), plt.show()  # type: ignore
