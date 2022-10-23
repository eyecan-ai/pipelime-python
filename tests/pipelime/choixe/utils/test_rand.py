import re
from typing import Optional
import numpy as np
import pytest

from pipelime.choixe.utils.rand import (
    ConstantFn,
    LinearFn,
    PiecewiseFn,
    QuadraticFn,
    RealFn,
    GenericFn,
    plot,
)


@pytest.fixture()
def mock_fn(monkeypatch):
    class RealFnMock(RealFn):
        def __init__(self) -> None:
            super().__init__()
            self.call_arg = []
            self.repr_called = 0

        def _call(self, x: np.ndarray) -> np.ndarray:
            self.call_arg.append(x)
            return x

        def _repr(self) -> str:
            self.repr_called += 1
            return "mock"

        def integrate(self) -> RealFn:
            raise NotImplementedError

        def invert(self, start: float, stop: float) -> Optional[RealFn]:
            raise NotImplementedError

        def shift(self, dx: float, dy: float) -> RealFn:
            raise NotImplementedError

    return RealFnMock


def _test_call(fn: RealFn, x: np.ndarray, expected: np.ndarray) -> None:
    result = fn(x)
    assert np.allclose(result, expected, rtol=1e-2, atol=0)


def _test_shift(fn: RealFn, x: np.ndarray, dx: float, dy: float) -> None:
    result = fn(x)
    shifted = fn.shift(dx, dy)
    _test_call(shifted, x + dx, result + dy)


def _test_integrate(fn: RealFn, x: np.ndarray, expected: np.ndarray) -> None:
    integrated = fn.integrate()
    _test_call(integrated, x, expected)


def _test_invert(
    fn: RealFn, x: np.ndarray, start: float, stop: float, invertible: bool
) -> None:
    inverted = fn.invert(start, stop)
    result = fn(x)
    if invertible:
        assert inverted is not None
        _test_call(inverted, result, x)
    else:
        assert inverted is None


class TestRealFn:
    @pytest.mark.parametrize(
        "x",
        [
            np.array([1, 2, 3]),
            np.array([1.0, 2.0, 3.0]),
            np.arange(100).reshape(10, 10),
            [1, 2, 3],
            [1.0, 2.0, 3.0],
            [1, 2, 3.0],
            [[1, 2, 3], [4, 5, 6]],
            10,
            10.0,
            True,
            False,
        ],
    )
    def test_call(self, mock_fn, x: np.ndarray) -> None:
        fn = mock_fn()  # type: ignore
        result = fn(x)

        if isinstance(x, np.ndarray):
            assert result.dtype == x.dtype
            assert result.shape == x.shape
            assert len(fn.call_arg) == 1
            assert fn.call_arg[0] is x

        elif isinstance(x, list):
            assert np.allclose(np.array(result), np.array(x))
            assert len(fn.call_arg) == 1
            assert np.allclose(fn.call_arg[0], np.array(x))

        else:
            assert result == float(x)
            assert len(fn.call_arg) == 1
            assert fn.call_arg[0] == float(x)

    @pytest.mark.parametrize(["x"], [[np.linspace(-10, 10, 1000)]])
    @pytest.mark.parametrize(
        ["dx", "dy"], [[0.0, 0.0], [1.0, 0.0], [0.0, 1.0], [1.0, 1.0]]
    )
    @pytest.mark.parametrize(
        ["fn", "y", "integral", "start", "stop", "invertible"],
        [
            [
                LinearFn(1, 0),
                np.linspace(-10, 10, 1000),
                np.linspace(-10, 10, 1000) ** 2 / 2,
                -10,
                10,
                True,
            ],
            [
                LinearFn(1, 1),
                np.linspace(-10, 10, 1000) + 1,
                np.linspace(-10, 10, 1000) ** 2 / 2 + 1 * np.linspace(-10, 10, 1000),
                -10,
                10,
                True,
            ],
            [
                LinearFn(0, 5),
                np.ones(1000) * 5,
                np.linspace(-10, 10, 1000) * 5,
                -10,
                10,
                False,
            ],
            [
                ConstantFn(-1),
                np.ones(1000) * -1,
                np.linspace(-10, 10, 1000) * -1,
                -10,
                10,
                False,
            ],
            [
                QuadraticFn(1, 2, 3),
                np.linspace(-10, 10, 1000) ** 2 + 2 * np.linspace(-10, 10, 1000) + 3,
                np.linspace(-10, 10, 1000) ** 3 / 3
                + np.linspace(-10, 10, 1000) ** 2
                + 3 * np.linspace(-10, 10, 1000),
                -10,
                10,
                False,
            ],
            [
                QuadraticFn(0.1, 3, -3),
                np.linspace(-10, 10, 1000) ** 2 * 0.1
                + 3 * np.linspace(-10, 10, 1000)
                - 3,
                np.linspace(-10, 10, 1000) ** 3 / 3 * 0.1
                + 3 * np.linspace(-10, 10, 1000) ** 2 / 2
                - 3 * np.linspace(-10, 10, 1000),
                -10,
                10,
                True,
            ],
            [
                QuadraticFn(0.1, 4, 5),
                np.linspace(-10, 10, 1000) ** 2 * 0.1
                + 4 * np.linspace(-10, 10, 1000)
                + 5,
                np.linspace(-10, 10, 1000) ** 3 / 3 * 0.1
                + 4 * np.linspace(-10, 10, 1000) ** 2 / 2
                + 5 * np.linspace(-10, 10, 1000),
                -10,
                10,
                True,
            ],
            [
                QuadraticFn(0, 4, 3),
                np.linspace(-10, 10, 1000) * 4 + 3,
                np.linspace(-10, 10, 1000) ** 2 * 4 / 2
                + 3 * np.linspace(-10, 10, 1000),
                -10,
                10,
                True,
            ],
            [
                GenericFn(lambda x: np.exp(x) - 20, -10, 10, steps=10000),
                np.exp(np.linspace(-10, 10, 1000)) - 20,
                np.exp(np.linspace(-10, 10, 1000))
                - 20 * np.linspace(-10, 10, 1000)
                - 1,
                -10,
                10,
                True,
            ],
        ],
    )
    def test_fn(
        self,
        fn: RealFn,
        x: np.ndarray,
        y: np.ndarray,
        dx: float,
        dy: float,
        integral: np.ndarray,
        start: float,
        stop: float,
        invertible: bool,
    ) -> None:
        _test_call(fn, x, y)
        _test_shift(fn, x, dx, dy)
        _test_integrate(fn, x, integral)
        _test_invert(fn, x, start, stop, invertible)
        assert isinstance(str(fn), str)


class TestPiecewiseFn:
    def test_no_segments(self) -> None:
        with pytest.raises(AssertionError):
            PiecewiseFn({})

    def test_call_one_segment(self) -> None:
        fn = PiecewiseFn({1.0: ConstantFn(1.0)})
        assert np.allclose(fn(np.array([0.0, 1.0, 2.0])), np.array([0.0, 1.0, 1.0]))

    def test_call_two_segments(self) -> None:
        fn = PiecewiseFn({1.0: ConstantFn(1.0), 2.0: ConstantFn(2.0)})
        assert np.allclose(
            fn(np.array([0.0, 1.0, 2.0, 3.0])), np.array([0.0, 1.0, 2.0, 2.0])
        )

    def test_repr(self) -> None:
        fn = PiecewiseFn({1.0: ConstantFn(1.0), 2.0: ConstantFn(2.0)})
        assert isinstance(repr(fn), str)

    @pytest.mark.parametrize(
        ["dx", "dy"],
        [
            [0.0, 0.0],
            [1.0, 0.0],
            [0.0, 1.0],
            [1.0, 1.0],
            [-1.0, 1.0],
            [1.0, -1.0],
            [-1.0, -1.0],
        ],
    )
    @pytest.mark.parametrize(
        "segments",
        [
            {1.0: ConstantFn(1.0)},
            {1.0: ConstantFn(1.0), 2.0: ConstantFn(2.0)},
            {1.0: ConstantFn(1.0), 2.0: ConstantFn(2.0), 3.0: ConstantFn(3.0)},
        ],
    )
    def test_shift(self, segments, dx, dy) -> None:
        start, stop = min(segments.keys()), max(segments.keys()) + 1
        fn = PiecewiseFn(segments)
        x = np.linspace(start, stop, 1000)
        _test_shift(fn, x, dx, dy)

    @pytest.mark.parametrize(
        ["fn", "x", "y"],
        [
            [
                PiecewiseFn({1.0: ConstantFn(1.0)}),
                np.array([0.0, 1.0, 2.0]),
                np.array([0.0, 0.0, 1.0]),
            ],
            [
                PiecewiseFn({1.0: ConstantFn(1.0), 2.0: ConstantFn(2.0)}),
                np.array([0.0, 1.0, 2.0, 3.0]),
                np.array([0.0, 0.0, 1.0, 3.0]),
            ],
            [
                PiecewiseFn({0.0: LinearFn(1.0, 0.0), 1.0: LinearFn(2.0, -1.0)}),
                np.array([0.0, 1.0, 2.0, 3.0]),
                np.array([0.0, 0.5, 2.5, 6.5]),
            ],
        ],
    )
    def test_integrate(self, fn: PiecewiseFn, x: np.ndarray, y: np.ndarray) -> None:
        _test_integrate(fn, x, y)

    @pytest.mark.parametrize(
        ["fn", "start", "stop"],
        [
            [PiecewiseFn({1.0: LinearFn(1.0, 0.0)}), 1.0, 2.0],
            [
                PiecewiseFn(
                    {
                        -1.0: LinearFn(1.0, 1.0),
                        0.0: GenericFn(lambda x: np.exp(x), 0.0, 1.0),
                    }
                ),
                -1.0,
                1.0,
            ],
        ],
    )
    def test_invert(self, fn, start, stop):
        _test_invert(fn, np.linspace(start, stop, 1000), start, stop, True)

    @pytest.mark.parametrize(
        ["fn", "start", "stop"],
        [
            [
                PiecewiseFn({0.0: LinearFn(1.0, 0.0), 1.0: LinearFn(-1.0, 2.0)}),
                0.0,
                2.0,
            ],
            [
                PiecewiseFn(
                    {-2.0: QuadraticFn(1.0, 0.0, 0.0), 4.0: LinearFn(1.0, 0.0)}
                ),
                -2.0,
                4.0,
            ],
        ],
    )
    def test_non_invertible(self, fn, start, stop):
        assert fn.invert(start, stop) is None

    @pytest.mark.parametrize(
        ["fn", "start", "stop"],
        [
            [
                PiecewiseFn(
                    {
                        0.0: LinearFn(1.0, 0.0),
                        0.5: ConstantFn(2.0),
                        1.0: LinearFn(1.0, 5.0),
                    }
                ),
                0.0,
                2.0,
            ],
        ],
    )
    def test_invertible_with_const(self, fn, start, stop):
        assert fn.invert(start, stop) is not None

    @pytest.mark.parametrize("piece", [LinearFn(1.0, 0.0), ConstantFn(1.0)])
    def test_parse_piece_realfn(self, piece) -> None:
        fn = PiecewiseFn.parse_piece(piece, 0.0, 1.0)
        assert fn is piece

    @pytest.mark.parametrize("piece", [0.5, 0.1, -0.5, -0.1])
    def test_parse_piece_float(self, piece) -> None:
        fn = PiecewiseFn.parse_piece(piece, 0.0, 1.0)
        assert isinstance(fn, ConstantFn)
        assert fn.q == piece

    @pytest.mark.parametrize("piece", [lambda x: x % 3])
    def test_parse_piece_callable(self, piece) -> None:
        fn = PiecewiseFn.parse_piece(piece, 0.0, 1.0)
        assert isinstance(fn, GenericFn)
        assert fn.fn is piece

    @pytest.mark.parametrize("piece", [10, "ciao", [1, 2, 3]])
    def test_parse_piece_raise(self, piece) -> None:
        with pytest.raises(TypeError):
            PiecewiseFn.parse_piece(piece, 0.0, 1.0)