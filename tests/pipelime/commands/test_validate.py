import pytest
from .test_general_base import TestGeneralCommandsBase


class TestValidate(TestGeneralCommandsBase):
    @pytest.mark.parametrize("nproc", [0, 1, 2])
    @pytest.mark.parametrize("prefetch", [2, 4])
    @pytest.mark.parametrize("max_samples", [0, -10, 5])
    def test_validate(self, minimnist_dataset, nproc, prefetch, max_samples):
        import io

        import pydash as py_
        import yaml

        from pipelime.commands import ValidateCommand
        from pipelime.commands.interfaces import SampleValidationInterface
        from pipelime.sequences import SamplesSequence

        # compute the validation schema
        params = {
            "input": {"folder": minimnist_dataset["path"]},
            "max_samples": max_samples,
            "grabber": f"{nproc},{prefetch}",
        }
        cmd = ValidateCommand.parse_obj(params)
        cmd()

        # apply the schema on the input dataset
        assert cmd.output_schema_def is not None
        outschema = repr(cmd.output_schema_def)
        outschema = yaml.safe_load(io.StringIO(outschema))
        sample_schema = py_.get(outschema, cmd.root_key_path)
        assert sample_schema is not None
        py_.set_(params, cmd.root_key_path, sample_schema)
        cmd = ValidateCommand.parse_obj(params)
        cmd()

        # validate using standard piping as well
        seq = SamplesSequence.from_underfolder(
            params["input"]["folder"]
        ).validate_samples(
            sample_schema=SampleValidationInterface.parse_obj(sample_schema)
        )
        seq.run(num_workers=nproc, prefetch=prefetch)

        # check the schema-to-cmdline converter
        params["root_key_path"] = ""
        cmd = ValidateCommand.parse_obj(params)
        cmd()
        assert cmd.output_schema_def is not None
        assert cmd.output_schema_def.schema_def == outschema["input"]["schema"]

        # test the dictionary flatting
        outschema = ValidateCommand.OutputCmdLineSchema(
            schema_def={"a": 42, "b": [True, [1, 2], {1: {"c": None}}]}
        )
        assert (
            repr(outschema) == "+a 42 +b[0] True +b[1][0] 1 +b[1][1] 2 +b[2].1.c None"
        )
