import pytest
import typing as t
from pathlib import Path


def _try_import_graphviz():
    try:
        import pygraphviz
    except ImportError:
        return False
    return True


@pytest.mark.skipif(not _try_import_graphviz(), reason="PyGraphviz not installed")
def test_draw(all_dags: t.Sequence[t.Mapping[str, t.Any]], tmp_path: Path):
    from pipelime.commands.piper import DrawCommand

    for dag in all_dags:
        if "dot" in dag:
            target_dot: Path = dag["dot"]
            outdot = tmp_path / target_dot.parent.name / target_dot.name
            outdot.parent.mkdir(parents=True, exist_ok=True)

            cmd = DrawCommand(
                **(dag["config"]),  # type: ignore
                output=outdot,  # type: ignore
                backend=DrawCommand.DrawBackendChoice.GRAPHVIZ,  # type: ignore
                data_max_width="/",  # type: ignore
                ellipsis_position=DrawCommand.EllipsesChoice.START,  # type: ignore
                show_command_names=True,  # type: ignore
            )
            cmd()

            # NOTE: output dots have different node names due to the use of absolute paths,
            # so we create here the images and compare them. We do not save a reference image
            # since graphviz may change the layout in the future. Also, we do not compare SVGs,
            # since graphviz writes as comments the names of the nodes.
            import pygraphviz as pgv

            g_ref = pgv.AGraph(str(target_dot)).draw(format="bmp", prog="dot")
            g_out = pgv.AGraph(str(outdot)).draw(format="bmp", prog="dot")
            assert g_ref is not None
            assert g_out is not None
            assert g_ref == g_out


def test_run(all_dags: t.Sequence[t.Mapping[str, t.Any]], tmp_path: Path):
    from pipelime.commands.piper import RunCommand

    for dag in all_dags:
        cmd = RunCommand(**(dag["config"]))
        cmd()

        # output folders now exist, so the commands should fail
        # when creating the output pipes
        cmd = RunCommand(**(dag["config"]))
        with pytest.raises(RuntimeError) as excinfo:
            cmd()


def test_port_forwarding(piper_folder: Path, tmp_path: Path):
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


def test_nested_dag(piper_folder: Path, tmp_path: Path):
    import pipelime.choixe.utils.io as choixe_io
    from pipelime.choixe import XConfig
    from pipelime.commands.piper import RunCommand
    from pipelime.sequences import SamplesSequence
    from ... import TestAssert

    dag_path = piper_folder / "nested" / "nested.yml"
    cfg = XConfig(choixe_io.load(dag_path), cwd=piper_folder / "nested")
    cfg = cfg.process({"folder": tmp_path.as_posix()}).to_dict()

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
