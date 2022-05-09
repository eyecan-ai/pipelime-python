from enum import Enum
from pathlib import Path
from typing import Optional

import typer
from typer import Option

piper = typer.Typer()


dag_help = "The DAG configuration file"
params_help = "The parameters configuration file"
draw_backend_help = "The backend to use for drawing the graph"
draw_open_help = "Open the generated image"
draw_output_help = "The output file"
compile_out_help = "The output file"


@piper.command()
def compile(
    dag_file: Path = Option(..., "-d", "--dag", help=dag_help),
    params_file: Path = Option(..., "-p", "--params", help=params_help),
    output_file: Optional[Path] = Option(None, "-o", "--output", help=compile_out_help),
):
    import rich

    from pipelime.choixe import XConfig
    from pipelime.piper.parsers.factory import DAGParserFactory

    dag = DAGParserFactory.get_parser().parse_file(dag_file, params_file)
    compiled = XConfig(dag.purged_dict())

    if output_file is not None:
        compiled.save_to(output_file)
    else:
        rich.print(compiled)


@piper.command()
def run():
    pass


class DrawBackendChoice(Enum):
    GRAPHVIZ = "graphviz"
    MERMAID = "mermaid"


@piper.command()
def draw(
    dag_file: Path = Option(..., "-d", "--dag", help=dag_help),
    params_file: Optional[Path] = Option(None, "-p", "--params", help=params_help),
    output_file: Optional[Path] = Option(None, "-o", "--output", help=draw_output_help),
    draw_backend: DrawBackendChoice = Option(
        "graphviz", "-b", "--backend", help=draw_backend_help
    ),
    open: bool = typer.Option(False, is_flag=True, help=draw_open_help),
):
    import os
    import platform
    import subprocess

    import cv2 as cv
    import numpy as np
    import rich

    from pipelime.piper.drawing.factory import NodesGraphDrawerFactory
    from pipelime.piper.graph import DAGNodesGraph
    from pipelime.piper.parsers.factory import DAGParserFactory

    def start_file(filename: str):
        if platform.system() == "Darwin":  # macOS
            subprocess.call(("open", filename))
        elif platform.system() == "Windows":  # Windows
            os.startfile(filename)
        else:  # linux variants #TODO: verify!
            subprocess.call(("xdg-open", filename))

    # DAG Model (abstraction layer to allow several parsing methods)
    dag = DAGParserFactory.get_parser().parse_file(
        cfg_file=dag_file, params_file=params_file
    )

    # Graph
    graph = DAGNodesGraph.build_nodes_graph(dag)

    # Drawn image
    graph_image: Optional[np.ndarray] = None

    # Draw with selected backend
    drawer = NodesGraphDrawerFactory.create(draw_backend.value)

    # Show or Write
    if output_file is not None:
        drawer.export(graph, output_file)
        if open:
            start_file(output_file)
        else:
            rich.print("graph image saved to:", output_file)
    else:
        graph_image = drawer.draw(graph=graph)
        cv.imshow("graph", cv.cvtColor(graph_image, cv.COLOR_RGB2BGR))
        cv.waitKey(0)


if __name__ == "__main__":
    piper()
