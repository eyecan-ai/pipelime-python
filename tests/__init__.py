# utility functions for tests
class TestUtils:
    @staticmethod
    def numpy_eq(x, y) -> bool:
        import numpy as np

        return np.array_equal(x, y, equal_nan=True)

    @staticmethod
    def has_torch():
        try:
            import torch
        except ImportError:
            return False
        return True


class TestAssert:
    @staticmethod
    def samples_equal(s1, s2):
        from pipelime.items import NumpyItem

        assert s1.keys() == s2.keys()
        for k, v1 in s1.items():
            v2 = s2[k]
            assert v1.__class__ == v2.__class__
            if isinstance(v1, NumpyItem):
                assert TestUtils.numpy_eq(v1(), v2())
            else:
                assert v1() == v2()
