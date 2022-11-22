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
