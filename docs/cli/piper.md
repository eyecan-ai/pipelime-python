# Piper

In the [previous section](overview.md) we talked about commands you can run from the command line.
A really special pipelime command is `run`, which executes a DAG (Directed Acyclic Graph) of commands using the **Piper** engine.

From [Wikipedia](https://en.wikipedia.org/wiki/Directed_acyclic_graph):
> a directed acyclic graph (DAG) is a directed graph with no directed cycles. That is, it consists of vertices and edges (also called arcs), with each edge directed from one vertex to another, such that following those directions will never form a closed loop.

Here, the nodes are pipelime command to execute, while the edges are matched input/output ports.
Piper takes as input any field having a key `piper_port` set to `PiperPortType.INPUT` and
as output any field having a key `piper_port` set to `PiperPortType.OUTPUT`.
Then, it compares inputs and outputs by converting them to string.
For example, `InputDatasetInterface` and `OutputDatasetInterface` are converted to their input and output folder paths, respectively.

Now let's see how to use it:

```bash
$ pipelime run help
```

```
>>>
━━━━━ Pipelime Command
                                               run
 (*, n: Mapping[str, Union[pipelime.piper.model.PipelimeCommand, Mapping[str, Union[Mapping[str,
  Any], NoneType]]]], include: Union[str, Sequence[str], NoneType] = None, exclude: Union[str,
        Sequence[str], NoneType] = None, t: Union[str, NoneType] = None, w: bool = True)
                              Executes a DAG of pipelime commands.

 Fields                  Description             Type                     Piper Port     Default
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 nodes / n               ▶ A DAG of commands     Mapping[str,             📥 INPUT       ✗
                         as a `<node>:           Union[pipelime.piper.m
                         <command>` mapping.     odel.PipelimeCommand,
                         The command can be a    Mapping[str,
                         `<name>: <args>`        Union[Mapping[str,
                         mapping, where          Any], NoneType]]]]
                         `<name>` is `pipe`,
                         `clone`, `split` etc,
                         while `<args>` is a
                         mapping of its
                         arguments.
 ━━━━━ PipelimeCommand
   (no parameters)

 include                 ▶ Nodes not in this     Union[str,               📐 PARAMETER   None
                         list are not run.       Sequence[str],
                                                 NoneType]

 exclude                 ▶ Nodes in this list    Union[str,               📐 PARAMETER   None
                         are not run.            Sequence[str],
                                                 NoneType]

 token / t               ▶ The execution         str                      📐 PARAMETER   None
                         token. If not
                         specified, a new
                         token will be
                         generated.

 watch / w               ▶ Monitor the           bool                     📐 PARAMETER   True
                         execution in the
                         current console.


                               pipelime.commands.piper.RunCommand
```
