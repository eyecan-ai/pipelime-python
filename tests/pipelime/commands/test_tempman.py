from pipelime.choixe.utils.io import PipelimeTmp
from pathlib import Path
import pytest
import contextlib


@contextlib.contextmanager
def undo_pltemp_dir():
    real_temp = PipelimeTmp.SESSION_TMP_DIR
    PipelimeTmp.SESSION_TMP_DIR = None
    try:
        yield
    finally:
        PipelimeTmp.SESSION_TMP_DIR = real_temp


class TestTempCommand:
    def _find_folder(self, name):
        temp_dir = PipelimeTmp.SESSION_TMP_DIR
        assert temp_dir is not None
        for d in temp_dir.glob("*"):
            if name == d.stem:
                return
        assert False, f"Temp folder {name} not found in {list(temp_dir.glob('*'))}"

    def _create_temp_folder(self, name):
        with pytest.raises(AssertionError):
            self._find_folder(name)
        PipelimeTmp.make_subdir(name)
        self._find_folder(name)

    def test_show_temp_folders(self, capfd):
        from pipelime.commands.tempman import TempCommand

        temp_folder = "show_test"
        self._create_temp_folder(temp_folder)

        TempCommand()()  # type: ignore

        # TODO: not able to get output from rich
        # assert str(PipelimeTmp.SESSION_TMP_DIR) in capfd.readouterr().out

    def test_delete_temp_folder(self):
        from pipelime.commands.tempman import TempCommand

        with undo_pltemp_dir():
            temp_folder = "delete_test"
            self._create_temp_folder(temp_folder)

            assert PipelimeTmp.SESSION_TMP_DIR is not None
            TempCommand(name=PipelimeTmp.SESSION_TMP_DIR.stem, force=True)()  # type: ignore
            assert not PipelimeTmp.SESSION_TMP_DIR.exists()
