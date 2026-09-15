import pytest


class TestPipelimeSymbolsHelper:
    @pytest.fixture
    def clean_helper(self):
        from pipelime.cli.utils import PipelimeSymbolsHelper as H

        saved = (
            list(H.extra_modules),
            dict(H.cached_modules),
            dict(H.cached_cmds),
            dict(H.cached_seq_ops),
            dict(H.cached_stages),
        )
        H.extra_modules = []
        H.cached_modules = {}
        H.cached_cmds = {}
        H.cached_seq_ops = {}
        H.cached_stages = {}
        yield H
        (
            H.extra_modules,
            H.cached_modules,
            H.cached_cmds,
            H.cached_seq_ops,
            H.cached_stages,
        ) = saved

    def test_reexported_symbols_are_not_duplicates(self, clean_helper, tmp_path):
        # a user module importing a pipelime stage/command (as any test module or
        # user script does) must not be reported as a duplicate definition
        module_file = tmp_path / "my_module.py"
        module_file.write_text(
            "from pipelime.commands import CloneCommand  # noqa: F401\n"
            "from pipelime.stages import StageEntity  # noqa: F401\n"
            "StageEntityAlias = StageEntity\n"
        )
        clean_helper.register_extra_module(module_file.as_posix())
        clean_helper.import_everything()

        stages = clean_helper.get_sample_stages()[("Sample Stage", "Sample Stages")]
        commands = clean_helper.get_pipelime_commands()[
            ("Pipelime Command", "Pipelime Commands")
        ]
        from pipelime.commands import CloneCommand
        from pipelime.stages import StageEntity

        assert stages["entity"] is StageEntity
        assert commands["clone"] is CloneCommand

    def test_different_classes_with_same_title_are_duplicates(
        self, clean_helper, tmp_path
    ):
        module_file = tmp_path / "my_dup_module.py"
        module_file.write_text(
            "from pipelime.stages import SampleStage\n"
            "class MyEntity(SampleStage, title='entity'):\n"
            "    def __call__(self, x):\n"
            "        return x\n"
        )
        clean_helper.register_extra_module(module_file.as_posix())
        with pytest.raises(ValueError, match="Duplicate stage `entity`"):
            clean_helper.import_everything()
