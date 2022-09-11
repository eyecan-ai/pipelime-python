import typing as t


class SubCommands:
    HELP = (("help", "h"), "show help for a command, an operator or a stage")
    LIST = (("list", "l", "ll"), "list all commands, operators or stages")
    LIST_CMDS = (("list-commands", "list-cmds", "list-cmd", "lc"), "list all commands")
    LIST_OPS = (("list-operators", "list-ops", "list-op", "lo"), "list all operators")
    LIST_STGS = (("list-stages", "list-stgs", "list-stg", "ls"), "list all stages")
    AUDIT = (("audit", "a"), "inspects configuration and context")
    WIZARD = (
        ("wizard", "wiz", "w"),
        "interactive wizard to fill command and general model configurations",
    )
    EXEC = (
        ("exec", "exe", "x", "e"),
        "execute a configuration where the command is the top-level key",
    )

    ALL_SUBC = [HELP, LIST, LIST_CMDS, LIST_OPS, LIST_STGS, AUDIT, WIZARD, EXEC]

    @classmethod
    def get_help(cls) -> str:
        return "\n".join(
            ["\b\nSPECIAL SUBCOMMANDS:"]
            + [
                el
                for sc in cls.ALL_SUBC
                for el in ("- " + ", ".join(sc[0]), f"    {sc[1]}")
            ]
        )

    @classmethod
    def get_autocompletions(cls) -> t.List[t.Tuple[str, str]]:
        return [(sc[0][0], sc[1]) for sc in cls.ALL_SUBC]
