# Installation

Install Pipelime using pip:

```bash
pip install pipelime-python
```

To be able to *draw* the dataflow graphs, you need the `draw` variant:

```bash
pip install pipelime-python[draw]
```

```{warning}
The `draw` variant needs `Graphviz` <https://www.graphviz.org/> installed on your system.
On Linux Ubuntu/Debian, you can install it with:

    sudo apt-get install graphviz graphviz-dev

Alternatively you can use `conda`:

    conda install --channel conda-forge pygraphviz

Please see the full options at <https://github.com/pygraphviz/pygraphviz/blob/main/INSTALL.txt>
```
