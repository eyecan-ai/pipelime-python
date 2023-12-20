import typing as t
from pathlib import Path
import pytest

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

    def _random_folder_name(self, n: int):
        return f"random_{n}"

    def _create_random_folder(self, path: Path, n: int, nfiles: int, nsubdir: int):
        random_folder = path / self._random_folder_name(n)
        random_folder.mkdir()
        for i in range(nfiles):
            self._create_random_file(random_folder, nfiles * n + i)
        for i in range(nsubdir):
            self._create_random_folder(random_folder, nsubdir * n + i, nfiles, 0)
        return random_folder

    def _check_random_file(self, path: t.Optional[Path], n: int):
        assert path is not None
        with open(path, "r") as f:
            assert f.read() == self._random_file_data(n)

    def _check_random_folder(
        self, path: t.Optional[Path], n: int, nfiles: int, nsubdir: int
    ):
        assert path is not None
        assert path.is_dir()
        assert len(list(path.iterdir())) == nfiles + nsubdir
        for i in range(nfiles):
            idx = nfiles * n + i
            filepath = path / self._random_file_name(idx)
            assert filepath.exists()
            assert filepath.is_file()
            self._check_random_file(filepath, idx)
        for i in range(nsubdir):
            idx = nsubdir * n + i
            subdirpath = path / self._random_folder_name(idx)
            assert subdirpath.exists()
            assert subdirpath.is_dir()
            self._check_random_folder(subdirpath, idx, nfiles, 0)

    def test_base_checkpoint(self):
        ckpt = pck.Checkpoint()
        lock = ckpt.create_lock("")
        assert hasattr(lock, "__enter__")
        assert hasattr(lock, "__exit__")

        # no raise
        ckpt.add_asset("", "")
        ckpt.add_asset("", "", lock)
        assert ckpt.get_asset("", "") is None
        assert ckpt.get_asset("", "", lock) is None
        ckpt.write_data("", "", 42)
        ckpt.write_data("", "", 42, lock)
        assert ckpt.read_data("", "", 42) == 42
        assert ckpt.read_data("", "", 42, lock) == 42

        ckpt_ns = ckpt.get_namespace("nmsp")
        assert isinstance(ckpt_ns, pck.CheckpointNamespace)
        assert ckpt_ns.checkpoint is ckpt
        assert ckpt_ns.namespace == "nmsp"

        lock = ckpt_ns.create_lock()
        assert hasattr(lock, "__enter__")
        assert hasattr(lock, "__exit__")

        # no raise
        ckpt_ns.add_asset("")
        ckpt_ns.add_asset("", lock)
        assert ckpt_ns.get_asset("") is None
        assert ckpt_ns.get_asset("", lock) is None
        ckpt_ns.write_data("", 42)
        ckpt_ns.write_data("", 42, lock)
        assert ckpt_ns.read_data("", 42) == 42
        assert ckpt_ns.read_data("", 42, lock) == 42

        ckpt_subns = ckpt_ns.get_namespace("subnmsp")
        assert isinstance(ckpt_subns, pck.CheckpointNamespace)
        assert ckpt_subns.checkpoint is ckpt
        assert "subnmsp" in ckpt_subns.namespace

    @pytest.mark.parametrize("try_link", [True, False])
    def test_local_checkpoint(self, try_link: bool, tmp_path: Path):
        ns_name, ns2_name = "nmsp", "nmsp2"
        random_files = [self._create_random_file(tmp_path, i) for i in range(2)]
        random_folders = [
            self._create_random_folder(tmp_path, i, 2, 2) for i in range(2)
        ]

        # write to checkpoint
        ckpt = pck.LocalCheckpoint(folder=tmp_path / "ckpt", try_link=try_link)
        for i, f in enumerate(random_files):
            ckpt.add_asset(ns_name, f)
            ckpt.write_data(ns_name, f"val[{i}]", i)

            # add again: no error
            ckpt.add_asset(ns_name, f)

        for i, f in enumerate(random_folders):
            ckpt.add_asset(ns_name, f)
            # add again: no error
            ckpt.add_asset(ns_name, f)

        # use a namespace class
        ckpt_ns2 = ckpt.get_namespace(ns2_name)
        for i, f in enumerate(random_files):
            ckpt_ns2.add_asset(f)
            ckpt_ns2.write_data(f"val.k{i}", i)

            # add again: no error
            ckpt_ns2.add_asset(f)

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

        for i in range(len(random_folders)):
            # ckpt write -> ckpt read
            path = ckpt.get_asset(ns_name, self._random_folder_name(i))
            self._check_random_folder(path, i, 2, 2)

            # ckpt write -> ckpt ns read
            path = ckpt_ns.get_asset(self._random_folder_name(i))
            self._check_random_folder(path, i, 2, 2)

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

        assert ckpt.read_data("null", "null", 42) == 42
        ckpt.add_asset(ns_name, "null")
        assert ckpt.get_asset(ns_name, "null") is None

        # lock
        with ckpt.create_lock(ns_name) as lock:
            ckpt.write_data(ns_name, "new_val", 42, lock)
            assert ckpt.read_data(ns_name, "new_val", None, lock) == 42

            ckpt_ns2.write_data("new_val", 54, lock)
            assert ckpt_ns2.read_data("new_val", None, lock) == 54
