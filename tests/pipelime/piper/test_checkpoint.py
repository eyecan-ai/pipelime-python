import typing as t
from pathlib import Path

import pipelime.piper.checkpoint as pck


class TestCheckpoint:
    def _random_file_name(self, n: int):
        return f"random_{n}.txt"

    def _random_file_data(self, n: int):
        return f"random text {n}"

    def _create_random_file(self, path: Path, n: int):
        random_file = path / self._random_file_name(n)
        with open(random_file, "w") as f:
            f.write(self._random_file_data(n))
        return random_file

    def _check_random_file(self, path: t.Optional[Path], n: int):
        assert path is not None
        with open(path, "r") as f:
            assert f.read() == self._random_file_data(n)

    def test_local_checkpoint(self, tmp_path: Path):
        ns_name, ns2_name = "nmsp", "nmsp2"
        random_files = [self._create_random_file(tmp_path, i) for i in range(2)]

        # write to checkpoint
        ckpt = pck.LocalCheckpoint(folder=tmp_path / "ckpt")
        for i, f in enumerate(random_files):
            ckpt.add_asset(ns_name, f)
            ckpt.write_data(ns_name, f"val[{i}]", i)

        # use a namespace class
        ckpt_ns2 = ckpt.get_namespace(ns2_name)
        for i, f in enumerate(random_files):
            ckpt_ns2.add_asset(f)
            ckpt_ns2.write_data(f"val.k{i}", i)

        # check
        ckpt_ns = ckpt.get_namespace(ns_name)
        for i in range(len(random_files)):
            # ckpt write -> ckpt read
            path = ckpt.get_asset(ns_name, self._random_file_name(i))
            self._check_random_file(path, i)
            assert ckpt.read_data(ns_name, f"val[{i}]", None) == i

            # ckpt write -> ckpt ns read
            path = ckpt_ns.get_asset(self._random_file_name(i))
            self._check_random_file(path, i)
            assert ckpt_ns.read_data(f"val[{i}]", None) == i

            # ckpt ns write -> ckpt ns read
            path = ckpt_ns2.get_asset(self._random_file_name(i))
            self._check_random_file(path, i)
            assert ckpt_ns2.read_data(f"val.k{i}", None) == i

            # ckpt ns write -> ckpt read
            path = ckpt.get_asset(ns2_name, self._random_file_name(i))
            self._check_random_file(path, i)
            assert ckpt.read_data(ns2_name, f"val.k{i}", None) == i

        # complex and non-existing data
        assert ckpt.read_data(ns_name, f"val[{len(random_files)}]", None) is None
        assert ckpt.read_data(ns_name, "val", None) == list(range(2))
        assert ckpt_ns.read_data(f"val[{len(random_files)}]", None) is None
        assert ckpt_ns.read_data("val", None) == list(range(2))

        assert ckpt_ns2.read_data(f"val.k{len(random_files)}", None) is None
        assert ckpt_ns2.read_data("val", None) == {
            f"k{i}": i for i in range(len(random_files))
        }
        assert ckpt.read_data(ns2_name, f"val.k{len(random_files)}", None) is None
        assert ckpt.read_data(ns2_name, "val", None) == {
            f"k{i}": i for i in range(len(random_files))
        }

        # lock
        with ckpt.create_lock(ns_name) as lock:
            ckpt.write_data(ns_name, "new_val", 42, lock)
            assert ckpt.read_data(ns_name, "new_val", None, lock) == 42

            ckpt_ns2.write_data("new_val", 54, lock)
            assert ckpt_ns2.read_data("new_val", None, lock) == 54
