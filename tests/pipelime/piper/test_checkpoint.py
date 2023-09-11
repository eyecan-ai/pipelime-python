from pathlib import Path

import pipelime.piper.checkpoint as pck


class TestCheckpoint:
    def _random_data(self, n: int):
        return f"random text {n}"

    def test_local_checkpoint(self, tmp_path: Path):
        ckpt = pck.LocalCheckpoint(folder=tmp_path / "ckpt")

        for i in range(2):
            random_file = tmp_path / f"random_{i}.txt"
            with open(random_file, "w") as f:
                f.write(self._random_data(i))

            ckpt.add_asset("nmsp", random_file)
            ckpt.write_data("nmsp", f"val[{i}]", i)

        ckpt_ns = ckpt.get_namespace("nmsp2")
        for i in range(2):
            random_file = tmp_path / f"random_{i}.txt"
            ckpt_ns.add_asset(random_file)
            ckpt_ns.write_data(f"val.k{i}", i)

        # check
        for i in range(2):
            path = ckpt.get_asset("nmsp", f"random_{i}.txt")
            assert path is not None
            with open(path, "r") as f:
                assert f.read() == self._random_data(i)
            assert ckpt.read_data("nmsp", f"val[{i}]", None) == i

            path = ckpt_ns.get_asset(f"random_{i}.txt")
            assert path is not None
            with open(path, "r") as f:
                assert f.read() == self._random_data(i)
            assert ckpt_ns.read_data(f"val.k{i}", None) == i

        assert ckpt.read_data("nmsp", "val[2]", None) is None
        assert ckpt.read_data("nmsp", f"val", None) == list(range(2))
        assert ckpt_ns.read_data("val.k2", None) is None
        assert ckpt_ns.read_data("val", None) == {"k0": 0, "k1": 1}

        with ckpt.create_lock("nmsp") as lock:
            ckpt.write_data("nmsp", "new_val", 42, lock)
            assert ckpt.read_data("nmsp", "new_val", None, lock) == 42

            ckpt_ns.write_data("new_val", 54, lock)
            assert ckpt_ns.read_data("new_val", None, lock) == 54
