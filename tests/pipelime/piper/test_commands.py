import typing as t
from pathlib import Path


def test_draw(complex_dag: t.Mapping, complex_dag_dot: Path, tmp_path: Path):
    # NOTE: output dots have different node names due to the use of absolute paths,
    # so we create here the images and compare them. We do not save a reference image
    # since graphviz may change the layout in the future. Also, we do not compare SVGs,
    # since graphviz writes as comments the names of the nodes.
    try:
        import pygraphviz as pgv
    except:
        import pytest

        pytest.skip("pygraphviz not installed")

    from filecmp import cmp
    from pipelime.commands.piper import DrawCommand

    outdot = tmp_path / "complex_dag.dot"
    cmd = DrawCommand(
        **complex_dag,  # type: ignore
        output=outdot,  # type: ignore
        backend=DrawCommand.DrawBackendChoice.GRAPHVIZ,  # type: ignore
        data_max_width="/",  # type: ignore
        ellipsis_position=DrawCommand.EllipsesChoice.START,  # type: ignore
        show_command_names=True,  # type: ignore
    )
    cmd()

    gref = pgv.AGraph(str(complex_dag_dot)).draw(format="bmp", prog="dot")
    gout = pgv.AGraph(str(outdot)).draw(format="bmp", prog="dot")
    assert gref is not None
    assert gout is not None
    assert gref == gout
