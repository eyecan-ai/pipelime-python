# utility functions for tests
class TestUtils:
    @staticmethod
    def numpy_eq(x, y) -> bool:
        import numpy as np

        return np.array_equal(x, y, equal_nan=True)

    @staticmethod
    def has_torch():
        try:
            import torch  # type: ignore
        except ImportError:
            return False
        return True

    @staticmethod
    def choixe_process(cfg_path, ctx_or_path):
        from typing import Mapping
        from pathlib import Path
        from pipelime.choixe import XConfig
        import pipelime.choixe.utils.io as choixe_io

        cfg = XConfig(choixe_io.load(Path(cfg_path)), cwd=Path(cfg_path).parent)
        ctx = (
            ctx_or_path
            if isinstance(ctx_or_path, Mapping)
            else (
                XConfig(choixe_io.load(Path(ctx_or_path)))
                if ctx_or_path and Path(ctx_or_path).exists()
                else None
            )
        )
        return cfg.process(ctx).to_dict()  # type: ignore


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
