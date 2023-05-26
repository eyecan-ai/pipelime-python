import pytest

from .test_general_base import TestGeneralCommandsBase


class TestFilterDuplicates(TestGeneralCommandsBase):
    @pytest.mark.parametrize(
        "keys,expected_length",
        [
            ["cfg", 1],
            ["numbers", 1],
            ["pose", 1],
            ["image", 20],
            ["label", 10],
            ["mask", 12],
            ["metadata", 20],
            ["points", 12],
            [["cfg", "image"], 20],
            [["cfg", "numbers", "pose"], 1],
            [["image", "label"], 20],
            [
                [
                    "cfg",
                    "numbers",
                    "pose",
                    "image",
                    "label",
                    "mask",
                    "metadata",
                    "points",
                ],
                20,
            ],
        ],
    )
    @pytest.mark.parametrize("algorithm", ["sha256"])
    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    def test_filter_duplicates(
        self,
        minimnist_dataset,
        keys,
        expected_length,
        algorithm,
        nproc,
        prefetch,
        tmp_path,
    ):
        from pipelime.commands import FilterDuplicatesCommand
        from pipelime.sequences import SamplesSequence

        params = {
            "input": minimnist_dataset["path"].as_posix(),
            "output": (tmp_path / "output").as_posix(),
            "grabber": f"{nproc},{prefetch}",
            "keys": keys,
            "algorithm": algorithm,
        }
        cmd = FilterDuplicatesCommand.parse_obj(params)
        cmd()

        seq = SamplesSequence.from_underfolder(params["output"])
        assert len(seq) == expected_length

    @pytest.mark.parametrize("keys", ["image"])
    @pytest.mark.parametrize(
        "algorithm,raises",
        [
            ["blake2b", False],
            ["blake2s", False],
            ["md5", False],
            ["sha1", False],
            ["sha224", False],
            ["sha256", False],
            ["sha384", False],
            ["sha3_224", False],
            ["sha3_256", False],
            ["sha3_384", False],
            ["sha3_512", False],
            ["sha512", False],
            ["shake_128", True],
            ["shake_256", True],
            ["myalgorithm", True],
        ],
    )
    def test_filter_duplicates_algorithm(
        self, minimnist_dataset, keys, algorithm, raises, tmp_path
    ):
        from contextlib import nullcontext

        from pipelime.commands import FilterDuplicatesCommand

        context = pytest.raises(ValueError) if raises else nullcontext()
        with context:
            params = {
                "input": minimnist_dataset["path"].as_posix(),
                "output": (tmp_path / "output").as_posix(),
                "keys": keys,
                "algorithm": algorithm,
            }
            cmd = FilterDuplicatesCommand.parse_obj(params)
            cmd()
