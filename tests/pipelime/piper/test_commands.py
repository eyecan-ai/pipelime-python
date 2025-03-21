import io
import typing as t
from pathlib import Path

import pytest
import yaml
from pydantic.v1 import BaseModel


class _GraphArgs(BaseModel):
    include: t.Union[str, t.Sequence[str], None]
    exclude: t.Union[str, t.Sequence[str], None]
    skip_on_error: bool
    start_from: t.Union[str, t.Sequence[str], None]
    stop_at: t.Union[str, t.Sequence[str], None]


class _DotOpts(BaseModel):
    args: _GraphArgs
    dot: str


class _DotsTestData(BaseModel):
    __root__: t.Sequence[_DotOpts]


def _try_import_graphviz():
    try:
        import pygraphviz
    except ImportError:
        return False
    return True


class TestCommands:
    def _gc_run(self, cmd):
        import gc

        gc.disable()
        gc.collect()
        count_start = gc.get_count()
        cmd()
        count_end = gc.get_count()
        gc.collect()
        gc.enable()

        return count_start, count_end

    def _gc_run_and_check(self, cmd, force_gc: bool):
        count_start, count_end = self._gc_run(cmd)

        if force_gc:
            assert count_end[0] == count_start[0]
        else:
            assert count_end[0] > count_start[0]
        assert count_end[1] == count_start[1]
        assert count_end[2] == count_start[2]

    @pytest.mark.skipif(not _try_import_graphviz(), reason="PyGraphviz not installed")
    def test_draw(self, all_dags: t.Sequence[t.Mapping[str, t.Any]], tmp_path: Path):
        from pipelime.commands.piper import DrawCommand

        for dag in all_dags:
            if "dot" in dag:
                with open(dag["dot"]) as f:
                    dots_test_data = _DotsTestData.parse_obj(yaml.safe_load(f))
                for idx, test_data in enumerate(dots_test_data.__root__):
                    target_dot = test_data.dot
                    outdot = tmp_path / str(idx) / "out.dot"
                    outdot.parent.mkdir(parents=True, exist_ok=True)

                    cmd = DrawCommand(
                        **(dag["config"]),  # type: ignore
                        output=outdot,  # type: ignore
                        backend=DrawCommand.DrawBackendChoice.GRAPHVIZ,  # type: ignore
                        data_max_width="/",  # type: ignore
                        ellipsis_position=DrawCommand.EllipsesChoice.START,  # type: ignore
                        show_command_names=True,  # type: ignore
                        **test_data.args.dict(),
                    )
                    cmd()

                    # NOTE: output dots have different node names due to the use of
                    # absolute paths, so we create here the images and compare them.
                    # We do not save a reference image since graphviz may change the
                    # layout in the future. Also, we do not compare SVGs, since graphviz
                    # writes as comments the names of the nodes.
                    import pygraphviz as pgv

                    # reading from string does not work, so we write to a file
                    with open(tmp_path / str(idx) / "target.dot", "w") as f:
                        f.write(target_dot)

                    g_ref = pgv.AGraph(str(tmp_path / str(idx) / "target.dot")).draw(
                        format="bmp", prog="dot"
                    )
                    g_out = pgv.AGraph(str(outdot)).draw(format="bmp", prog="dot")
                    assert g_ref is not None
                    assert g_out is not None
                    assert g_ref == g_out

    @pytest.mark.parametrize(
        "watch", [True, False, "rich", "tqdm", Path("cmdout.json"), None]
    )
    def test_run_dag(
        self,
        all_dags: t.Sequence[t.Mapping[str, t.Any]],
        watch: t.Union[bool, str, Path, None],
        tmp_path: Path,
    ):
        import shutil

        from pipelime.choixe.utils.io import PipelimeTmp
        from pipelime.commands.piper import RunCommand

        if isinstance(watch, Path):
            watch = tmp_path / watch

        for dag in all_dags:
            cmd = RunCommand(watch=watch, **(dag["config"]))  # type: ignore
            cmd()

            if isinstance(watch, Path):
                assert watch.exists()

            # output folders now exist, so the commands should fail
            # when creating the output pipes
            cmd = RunCommand(**(dag["config"]))
            with pytest.raises(ValueError):
                cmd()

            if PipelimeTmp.SESSION_TMP_DIR is not None:
                for path in PipelimeTmp.SESSION_TMP_DIR.iterdir():
                    if path.is_dir():
                        shutil.rmtree(path, ignore_errors=True)

    def test_port_forwarding(self, piper_folder: Path, tmp_path: Path):
        import pipelime.choixe.utils.io as choixe_io
        from pipelime.choixe import XConfig
        from pipelime.commands.piper import RunCommand

        dag_path = piper_folder / "nested" / "simple.yml"
        cfg = XConfig(choixe_io.load(dag_path))
        cfg = cfg.process({"folder": tmp_path.as_posix()}).to_dict()

        cmd = RunCommand(**cfg)
        assert cmd.get_inputs() == {
            "merge.cat.inputs[0]": (tmp_path / "first").as_posix(),
            "merge.cat.inputs[1]": (tmp_path / "second").as_posix(),
            "merge.cat.inputs[2]": (tmp_path / "third").as_posix(),
        }
        assert cmd.get_outputs() == {
            "split-all.split.splits[0]": (tmp_path / "split0").as_posix(),
            "split-all.split.splits[1]": (tmp_path / "split1").as_posix(),
        }
        assert cmd.piper_graph.num_nodes == 8
        assert cmd.piper_graph.num_operation_nodes == 2
        assert cmd.piper_graph.num_data_nodes == 6

    def test_nested_dag(self, piper_folder: Path, tmp_path: Path):
        from pipelime.commands.piper import RunCommand
        from pipelime.sequences import SamplesSequence

        from ... import TestAssert, TestUtils

        dag_path = piper_folder / "nested" / "nested.yml"
        cfg = TestUtils.choixe_process(dag_path, {"folder": tmp_path.as_posix()})

        cmd = RunCommand(**cfg)
        cmd()

        # recreate outputs here
        merged = (
            SamplesSequence.from_underfolder(tmp_path / "first")
            .cat(SamplesSequence.from_underfolder(tmp_path / "second"))
            .cat(SamplesSequence.from_underfolder(tmp_path / "third"))
        )
        split_0 = merged[:10]
        split_1 = merged[10:]
        inverted = split_1 + split_0

        dag_output = SamplesSequence.from_underfolder(tmp_path / "inverted")
        assert len(dag_output) == len(inverted)
        for gt, pred in zip(inverted, dag_output):
            TestAssert.samples_equal(gt, pred)

    def test_dag_gc(self, piper_folder: Path):
        from pipelime.choixe.utils.io import PipelimeTmp
        from pipelime.commands.piper import RunCommand

        from ... import TestUtils

        PipelimeTmp.SESSION_TMP_DIR = None

        dag_path = piper_folder / "gc_test" / "dag.yml"
        cfg = TestUtils.choixe_process(dag_path, None)

        cmd = RunCommand(**cfg, force_gc=False)
        nogc_start, nogc_end = self._gc_run(cmd)

        PipelimeTmp.SESSION_TMP_DIR = None

        reprocessed_dag = TestUtils.choixe_process(dag_path, None)
        cmd = RunCommand(**reprocessed_dag, force_gc=True)
        gc_start, gc_end = self._gc_run(cmd)

        # assert nogc_start[0] == gc_start[0]
        assert nogc_end[0] > gc_end[0]
        assert nogc_end[1] == nogc_start[1]
        assert nogc_end[2] == nogc_start[2]
        assert gc_end[1] == gc_start[1]
        assert gc_end[2] == gc_start[2]

    def test_force_gc(self):
        from pipelime.piper import command

        @command(title="testcm_no_set")
        def _testcm_no_set():
            a = [1, 2, 3]
            b = "test"
            c = {"a": a, "b": b, "c": 42.5}  # noqa: F841, W0612

        @command(title="testcm_false", force_gc=False)
        def _testcm_false():
            a = [1, 2, 3]
            b = "test"
            c = {"a": a, "b": b, "c": 42.5}  # noqa: F841, W0612

        @command(title="testcm_true", force_gc=True)
        def _testcm_true():
            a = [1, 2, 3]
            b = "test"
            c = {"a": a, "b": b, "c": 42.5}  # noqa: F841, W0612

        # force_gc not set
        cmd = _testcm_no_set()
        self._gc_run_and_check(cmd, False)

        # force_gc = False
        cmd = _testcm_false()
        self._gc_run_and_check(cmd, False)

        # force_gc = True
        cmd = _testcm_true()
        self._gc_run_and_check(cmd, True)
