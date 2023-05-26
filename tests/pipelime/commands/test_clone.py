import pytest
from .test_general_base import TestGeneralCommandsBase

from ... import TestAssert


class TestClone(TestGeneralCommandsBase):
    @pytest.mark.parametrize("lazy", [True, False])
    @pytest.mark.parametrize("ignore_extra_keys", [True, False])
    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [1, 2, 4])
    @pytest.mark.parametrize("skip_empty", [True, False])
    def test_clone(
        self,
        minimnist_dataset,
        lazy: bool,
        ignore_extra_keys: bool,
        nproc: int,
        prefetch: int,
        skip_empty: bool,
        tmp_path,
    ):
        import shutil

        from pipelime.commands import CloneCommand
        from pipelime.sequences import SamplesSequence

        partial_input = tmp_path / "partial_input"
        shutil.copytree(
            minimnist_dataset["path"],
            partial_input,
            ignore=shutil.ignore_patterns("*01_*", "*10_*"),
        )
        len_out = (
            minimnist_dataset["len"] - 2 if skip_empty else minimnist_dataset["len"]
        )

        cmd = CloneCommand.parse_obj(
            {
                "input": {
                    "folder": partial_input,
                    "skip_empty": skip_empty,
                    "schema": {
                        "sample_schema": self.minimnist_partial_schema
                        if ignore_extra_keys
                        else self.minimnist_full_schema,
                        "ignore_extra_keys": ignore_extra_keys,
                        "lazy": lazy,
                    },
                },
                "output": {
                    "folder": tmp_path / "output",
                    "serialization": {
                        "override": {"DEEP_COPY": None},
                        "disable": {"MetadataItem": ["HARD_LINK", "DEEP_COPY"]},
                        "keys": {"image": "HARD_LINK"},
                    },
                },
                "grabber": f"{nproc},{prefetch}",
            }
        )

        if lazy and not skip_empty:
            # schema validation fails
            with pytest.raises(Exception) as exc_info:
                cmd()
            if nproc == 0:
                assert isinstance(exc_info.value, ValueError)
            return

        cmd()

        src = SamplesSequence.from_underfolder(partial_input)
        dst = SamplesSequence.from_underfolder(tmp_path / "output")
        assert len(src) == minimnist_dataset["len"]
        assert len(dst) == len_out

        iout = 0
        for iin in range(len(src)):
            if skip_empty and iin in [1, 10]:
                continue
            TestAssert.samples_equal(src[iin], dst[iout])
            iout += 1
