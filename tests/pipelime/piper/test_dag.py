import typing as t
from pathlib import Path

import pytest
from pydantic import Field, ValidationError

import pipelime.commands.interfaces as pl_interfaces
from pipelime.commands.piper import T_NODES, DagBaseCommand, PiperDAG, piper_dag
from pipelime.piper import PiperPortType
from pipelime.sequences import SamplesSequence


def _try_import_graphviz():
    try:
        import pygraphviz
    except ImportError:
        return False
    return True


class DAG(DagBaseCommand):
    """Simple Python DAG, with the following nodes:
    - slice: slice the minimnist dataset
    - copy: copy the sliced dataset
    - remap: change the name of image items
    - concat: concatenate the remapped dataset and the sliced
    """

    input: pl_interfaces.InputDatasetInterface = (
        pl_interfaces.InputDatasetInterface.pyd_field(
            alias="i",
            description="The input of the DAG",
            piper_port=PiperPortType.INPUT,
        )
    )

    output: Path = Field(
        ...,
        alias="o",
        description="The output of the DAG",
        piper_port=PiperPortType.OUTPUT,
    )

    subsample: int = Field(..., description="Number of samples to take.")

    key_image_item: str = Field(..., description="Key for image item.")

    @property
    def input_mapping(self) -> t.Optional[t.Mapping[str, str]]:
        return {"slice.slice.input": "input"}

    @property
    def output_mapping(self) -> t.Optional[t.Mapping[str, str]]:
        return {"cat.cat.output": "output"}

    def create_graph(self) -> T_NODES:
        from pipelime.commands import (
            CloneCommand,
            ConcatCommand,
            MapCommand,
            SliceCommand,
        )
        from pipelime.stages import StageRemap

        dir_out_split = self.folder_debug / "out_split"
        cmd_split = SliceCommand.lazy()(
            input=self.input, output=dir_out_split, slice=self.subsample
        )  # type: ignore

        dir_out_copy = self.folder_debug / "out_copy"
        cmd_copy = CloneCommand.lazy()(
            input=dir_out_split, output=dir_out_copy  # type: ignore
        )

        dir_out_remap = self.folder_debug / "out_remap"
        cmd_map = MapCommand.lazy()(
            input=dir_out_copy,
            output=dir_out_remap,
            stage=StageRemap(remap={self.key_image_item: "image_new"}),  # type: ignore
        )

        cmd_cat = ConcatCommand.lazy()(
            inputs=[dir_out_remap, dir_out_split], output=self.output  # type: ignore
        )

        graph = {
            "slice": cmd_split,
            "copy": cmd_copy,
            "remap": cmd_map,
            "cat": cmd_cat,
        }

        return graph


@piper_dag
class DecoratedDAG(PiperDAG, title="deco-dag"):
    """Simple Python DAG, with the following nodes:
    - slice: slice the minimnist dataset
    - copy: copy the sliced dataset
    - remap: change the name of image items
    - concat: concatenate the remapped dataset and the sliced
    """

    input: pl_interfaces.InputDatasetInterface = (
        pl_interfaces.InputDatasetInterface.pyd_field(
            alias="i",
            description="The input of the DAG",
            piper_port=PiperPortType.INPUT,
        )
    )

    output: Path = Field(
        ...,
        alias="o",
        description="The output of the DAG",
        piper_port=PiperPortType.OUTPUT,
    )
    subsample: int = Field(..., description="Number of samples to take.")

    key_image_item: str = Field(..., description="Key for image item.")

    @property
    def input_mapping(self) -> t.Optional[t.Mapping[str, str]]:
        return {"slice.slice.input": "input"}

    @property
    def output_mapping(self) -> t.Optional[t.Mapping[str, str]]:
        return {"cat.cat.output": "output"}

    def create_graph(self, folder_debug: Path) -> T_NODES:
        from pipelime.commands import (
            CloneCommand,
            ConcatCommand,
            MapCommand,
            SliceCommand,
        )
        from pipelime.stages import StageRemap

        dir_out_split = folder_debug / "out_split"
        cmd_split = SliceCommand.lazy()(
            input=self.input, output=dir_out_split, slice=self.subsample
        )  # type: ignore

        dir_out_copy = folder_debug / "out_copy"
        cmd_copy = CloneCommand.lazy()(
            input=dir_out_split, output=dir_out_copy  # type: ignore
        )

        dir_out_remap = folder_debug / "out_remap"

        cmd_map = MapCommand.lazy()(
            input=dir_out_copy,
            output=dir_out_remap,
            stage=StageRemap(remap={self.key_image_item: "image_new"}),  # type: ignore
        )

        cmd_cat = ConcatCommand.lazy()(
            inputs=[dir_out_remap, dir_out_split], output=self.output  # type: ignore
        )

        graph = {
            "slice": cmd_split,
            "copy": cmd_copy,
            "remap": cmd_map,
            "cat": cmd_cat,
        }

        return graph


def _create_dag(
    minimnist_dataset: dict,
    decorated: bool,
    slice: int,
    output: t.Optional[Path],
    debug_folder: t.Optional[Path] = None,
    include: t.Optional[t.Sequence[str]] = None,
    exclude: t.Optional[t.Sequence[str]] = None,
    skip_on_error: bool = False,
):
    if decorated:
        return DecoratedDAG(  # type: ignore
            folder_debug=debug_folder,  # type: ignore
            properties={  # type: ignore
                "input": minimnist_dataset["path"],
                "output": Path(__file__) if output is None else output / "dag_output",
                "key_image_item": minimnist_dataset["image_keys"][0],
                "subsample": slice,
            },
            include=include,  # type: ignore
            exclude=exclude,  # type: ignore
            skip_on_error=skip_on_error,  # type: ignore
        )

    dag = DAG(
        input=minimnist_dataset["path"],
        output=Path(__file__) if output is None else output / "dag_output",
        key_image_item=minimnist_dataset["image_keys"][0],
        subsample=slice,
        folder_debug=debug_folder,
        include=include,  # type: ignore
        exclude=exclude,  # type: ignore
        skip_on_error=skip_on_error,  # type: ignore
    )  # type: ignore
    return dag


class TestDAG:
    @pytest.mark.parametrize("decorated", [True, False])
    @pytest.mark.parametrize(
        ["include", "exclude", "effective_nodes"],
        [
            (None, None, {"slice", "copy", "remap", "cat"}),
            ([], [], set()),
            (["slice"], None, {"slice"}),
            (None, ["slice"], {"copy", "remap", "cat"}),
            ([], ["slice"], set()),
            (["slice", "copy"], ["slice"], {"copy"}),
            (["c*"], [], {"copy", "cat"}),
            (None, ["?a*"], {"slice", "copy", "remap"}),
            (["*e*"], ["*map"], {"slice"}),
        ],
    )
    def test_include_exclude(
        self,
        minimnist_dataset: dict,
        decorated: bool,
        include: t.Optional[t.List[str]],
        exclude: t.Optional[t.List[str]],
        effective_nodes: t.Set[str],
        tmp_path: Path,
    ):
        dag = _create_dag(
            minimnist_dataset,
            decorated=decorated,
            slice=4,
            output=tmp_path,
            debug_folder=None,
            include=include.copy() if include is not None else None,
            exclude=exclude.copy() if exclude is not None else None,
        )

        assert dag.piper_graph.num_operation_nodes == len(effective_nodes)

        nodes = {node.name for node in dag.piper_graph.operations_graph.raw_graph.nodes}
        assert nodes == effective_nodes

    @pytest.mark.parametrize("decorated", [True, False])
    def test_skip_on_error(self, minimnist_dataset: dict, decorated: bool):
        dag = _create_dag(
            minimnist_dataset,
            decorated=decorated,
            slice=4,
            output=None,
            debug_folder=None,
            skip_on_error=True,
        )
        assert dag.piper_graph.num_operation_nodes == 3

        nodes = {node.name for node in dag.piper_graph.operations_graph.raw_graph.nodes}
        assert nodes == {"slice", "copy", "remap"}

        dag = _create_dag(
            minimnist_dataset,
            decorated=decorated,
            slice=4,
            output=None,
            debug_folder=None,
            skip_on_error=False,
        )
        with pytest.raises(ValidationError):
            _ = dag.piper_graph

    @pytest.mark.parametrize("decorated", [True, False])
    @pytest.mark.parametrize("slice", [2, 4, 8])
    def test_run(
        self, minimnist_dataset: dict, decorated: bool, slice: int, tmp_path: Path
    ):
        size = minimnist_dataset["len"]
        path_out = tmp_path / "dag_output"

        dag = _create_dag(
            minimnist_dataset,
            decorated=decorated,
            slice=slice,
            output=tmp_path,
            debug_folder=None,
        )
        dag.run()

        out = SamplesSequence.from_underfolder(path_out)

        assert len(out) == 2 * (size - slice)

    @pytest.mark.skipif(not _try_import_graphviz(), reason="PyGraphviz not installed")
    @pytest.mark.parametrize("decorated", [True, False])
    def test_draw(self, minimnist_dataset: dict, decorated: bool, tmp_path: Path):
        dag = _create_dag(
            minimnist_dataset,
            decorated=decorated,
            slice=0,
            output=tmp_path,
            debug_folder=None,
        )
        file_draw_dag = tmp_path / "my_dag_draw.png"
        dag.draw_graph(output=file_draw_dag)

        assert file_draw_dag.exists()

    def test_run_exception(self, tmp_path: Path):
        dag = DAG(
            input=tmp_path / "input",
            output=tmp_path / "input",
            key_image_item="",
            subsample=1,
        )  # type: ignore

        with pytest.raises(RuntimeError):
            dag.run()

    @pytest.mark.parametrize("decorated", [True, False])
    @pytest.mark.parametrize("is_none", [True, False])
    def test_instantiate_folder_debug(
        self, minimnist_dataset: dict, decorated: bool, tmp_path: Path, is_none: bool
    ):
        debug_folder = None if is_none else tmp_path / "dag_debug"

        dag = _create_dag(
            minimnist_dataset,
            decorated=decorated,
            slice=0,
            output=tmp_path,
            debug_folder=debug_folder,
        )
        dag.run()
        if is_none:
            assert dag.folder_debug.exists()
        else:
            assert debug_folder.exists()  # type: ignore

    @pytest.mark.parametrize("decorated", [True, False])
    def test_cleanup_temp_folder_debug(
        self,
        minimnist_dataset: dict,
        decorated: bool,
        tmp_path: Path,
    ):
        from pipelime.commands import TempCommand
        from pipelime.choixe.utils.io import PipelimeTmp

        dag = _create_dag(
            minimnist_dataset, decorated=decorated, slice=1, output=tmp_path
        )
        dag.run()
        assert dag.folder_debug.exists()

        TempCommand(name=PipelimeTmp.SESSION_TMP_DIR.stem, force=True)()  # type: ignore

        assert not dag.folder_debug.exists()

    @pytest.mark.parametrize("decorated", [True, False])
    def test_not_cleanup_folder_debug(
        self,
        minimnist_dataset: dict,
        decorated: bool,
        tmp_path: Path,
    ):
        debug_dir = tmp_path / "debug"
        dag = _create_dag(
            minimnist_dataset,
            decorated=decorated,
            slice=1,
            output=tmp_path,
            debug_folder=debug_dir,
        )
        dag.run()
        assert dag.folder_debug == debug_dir
        assert dag.folder_debug.exists()

        del dag

        assert debug_dir.exists()

    @pytest.mark.parametrize("slice", [2, 4, 8])
    @pytest.mark.parametrize("size", [10, 20])
    def test_nested_dag(
        self, piper_folder: Path, tmp_path: Path, size: int, slice: int
    ):
        from pipelime.commands.piper import RunCommand

        from ... import TestUtils

        dag_yaml_path = piper_folder / "python_dags" / "nested.yml"
        output_path = tmp_path / "output_dag"
        cfg = TestUtils.choixe_process(
            dag_yaml_path,
            {
                "folder": tmp_path.as_posix(),
                "path_dag": Path(__file__).as_posix(),
                "output": output_path.as_posix(),
                "slice": slice,
                "size": size,
            },
        )

        cmd = RunCommand(**cfg)
        cmd()

        out = SamplesSequence.from_underfolder(output_path)

        assert len(out) == 2 * (size - slice)
