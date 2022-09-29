# Debugging In VS Code

We have already seen how to write a [custom pipelime command](../operations/commands.md).
Since any command is run through the pipelime CLI, to debug your code you have to call it from the CLI as well.
This boild down to launch the module `pipelime.cli.main` from python, passing the command name and the arguments you want to test. For example, in VS Code you can add to `launch.json` the following configuration:

```json
{
    "name": "$configuration_name",
    "type": "python",
    "request": "launch",
    "module": "pipelime.cli.main",
    "justMyCode": true,
    "args": [ "$command_name", "$arg0" , "$arg1", "..." ],
}
```
