# The `tmp` Command

Though the default behavior of Pipelime is to delete any temporary data, sometimes
the folders may not be deleted and accumulate. For instance, you may have used the `-t` flag
or directly called some Choixe utilities (see the step about
[custom commands](./custom_commands.md#temporary-folders-persistency) for more details).
In such cases, you can use the `tmp` command to show and delete the temporary data created by Pipelime.

## Show Temporary Data

To show the temporary data created by Pipelime, run the `tmp` command with no arguments:

```bash
$ pipelime tmp
```

You should see a list of paths and sizes sorted by accessed time.
Other sorting methods and filters are available:
- `+s name` / `+s size` / `+s time`: sort by name/size/time
- `+t ctime` / `+t mtime` / `+t atime`: show creation/modification/access time

## Delete Temporary Data

You can delete some or all temporary folders by passing any other option to the `tmp` command.
Unless you pass the `+f` / `+force` flag as well, you will be prompted to confirm the deletion.

To just clear all folders, run:

```bash
$ pipelime tmp +all
```

To delete folders including a specific substring in their name, use the `+n` / `+name` flag:

```bash
$ pipelime tmp +n 21kthgl1
```

To restrict to certain sizes, set the `+m` / `+min_size` and/or `+M` / `+max_size` parameters, eg:

```bash
$ pipelime tmp +m 1G
```

To restrict to certain dates, use the ISO-8601 format with the following flags:
- `+a` / `+after`: after a date-time, eg `2020-04-01`, `2020-04-01T12:21` or `12:21` (today)
- `+b` / `+before`: before a date-time
- `da` / `delta_after`: after `now - delta_after`, eg `P1D`, `PT1H` or `P2DT1H30M`
- `db` / `delta_before`: before `now - delta_before`

Finally, on platforms where users share the same temporary directory, you can clear the folders created by other users as well, provided that you have the right permissions:
- `+u` / `+user`: clear only the folders of one or more users, eg `pipelime tmp +u user1 +u user2`
- `+au` / `+all_users`: clear all folders, regardless of the user
