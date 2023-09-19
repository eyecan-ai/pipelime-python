import typing as t
from abc import ABC, abstractmethod


def parse_pipelime_cli(
    command_args: t.Sequence[str],
) -> t.Tuple[t.Mapping[str, t.Any], t.Mapping[str, t.Any]]:
    """Parses a pipelime command line and returns
    a tuple with (config_options, context_options)
    """
    try:
        cli_state = CLIParserHoldState()
        for idx, token in enumerate(command_args):
            cli_state = cli_state.process_token(token)
        cli_state.close()
    except Exception as e:
        try:
            hints = [
                f"Error raised while parsing token {token}",  # type: ignore
                " ".join(command_args),
                "─" * (sum((len(a) + 1) for a in command_args[:idx]))  # type: ignore
                + "⌃" * len(token),  # type: ignore
            ]
        except NameError:  # pragma: no cover
            hints = []
        if isinstance(e, CLIParsingError):
            raise CLIParsingError(*e.message, hints=hints + list(e.hints))
        raise CLIParsingError(str(e), hints=hints) from e
    return cli_state.config_options, cli_state.context_options


class CLIParsingError(Exception):
    """Exception raised when an error occurs during CLI parsing"""

    def __init__(
        self, *args, hints: t.Union[str, t.Sequence[str], None] = None, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.message = args
        self.hints = (
            [hints] if isinstance(hints, str) else ([] if hints is None else hints)
        )

    def rich_print(self):  # pragma: no cover
        from pipelime.cli.pretty_print import print_error, print_warning

        for m in self.args:
            print_error(m)
        for m in self.hints:
            print_warning(m)


class CLISpecialChars:
    @staticmethod
    def assignment():
        return ("=",)

    @staticmethod
    def config():
        # MUST BE SORTED FROM THE LONGEST TO THE SHORTEST
        return ("++", "+")

    @staticmethod
    def context():
        # MUST BE SORTED FROM THE LONGEST TO THE SHORTEST
        return ("@@", "@")

    @staticmethod
    def ctx_start():
        return ("//",)


class CLIParserState(ABC):
    def __init__(
        self,
        cfg_opts: t.Optional[t.Dict[str, t.Any]] = None,
        ctx_opts: t.Optional[t.Dict[str, t.Any]] = None,
        ctx_started: bool = False,
    ):
        self.cfg_opts = cfg_opts or {}
        self.ctx_opts = ctx_opts or {}
        self.ctx_started = ctx_started

    @property
    def config_options(self):
        return self.cfg_opts

    @property
    def context_options(self):
        return self.ctx_opts

    @abstractmethod
    def process_token(self, token: str) -> "CLIParserState":
        pass

    @abstractmethod
    def close(self):
        pass


class CLIParserHoldState(CLIParserState):
    def __init__(
        self,
        last_key_name_iscfg: t.Optional[t.Tuple[str, bool]] = None,
        cfg_opts: t.Optional[t.Dict[str, t.Any]] = None,
        ctx_opts: t.Optional[t.Dict[str, t.Any]] = None,
        ctx_started: bool = False,
    ):
        super().__init__(cfg_opts, ctx_opts, ctx_started)
        self.last_key_name_iscfg = last_key_name_iscfg

    def process_token(self, token: str) -> CLIParserState:
        # context starts
        if token in CLISpecialChars.ctx_start():
            return CLIParserHoldState(None, self.cfg_opts, self.ctx_opts, True)

        # config option
        if not self.ctx_started and token.startswith(CLISpecialChars.config()):
            opt, val = self._process_key_arg(token)
            cli_state = CLIParserExpectingCfgValue(
                opt, self.cfg_opts, self.ctx_opts, self.ctx_started
            )
            if val is not None:
                return cli_state.process_token(val)
            return cli_state

        # context option
        if (
            self.ctx_started and token.startswith(CLISpecialChars.config())
        ) or token.startswith(CLISpecialChars.context()):
            opt, val = self._process_key_arg(token)
            cli_state = CLIParserExpectingCtxValue(
                opt, self.cfg_opts, self.ctx_opts, self.ctx_started
            )
            if val is not None:
                return cli_state.process_token(val)
            return cli_state

        # value
        if self.last_key_name_iscfg:
            if self.last_key_name_iscfg[1]:
                cli_state = CLIParserExpectingCfgValue(
                    self.last_key_name_iscfg[0],
                    self.cfg_opts,
                    self.ctx_opts,
                    self.ctx_started,
                )
            else:
                cli_state = CLIParserExpectingCtxValue(
                    self.last_key_name_iscfg[0],
                    self.cfg_opts,
                    self.ctx_opts,
                    self.ctx_started,
                )
            return cli_state.process_token(token)

        raise CLIParsingError(f"Unexpected token: `{token}`")

    def close(self):
        return

    def _process_key_arg(self, token: str):
        opt, val = token, None
        for char in (  # pragma: no branch
            CLISpecialChars.config() + CLISpecialChars.context()
        ):
            if token.startswith(char):
                opt = token[len(char) :]  # noqa: E203
                break

        for char in CLISpecialChars.assignment():
            if char in opt:
                opt, _, val = opt.partition(char)
                break

        if opt.endswith(".") or ".." in opt or ".[" in opt:
            raise CLIParsingError(
                f"Invalid key path: `{opt}`",
                hints=[
                    "Remember: Bash and other shells want the choixe's "
                    "dollar sign (`$`) escaped! Try with single quotes or backslash.",
                    "For example:",
                    "bash/zsh: +operations.map.$model => '+operations.map.$model'",
                    r"zsh: +operations.map.$model => +operations.map.\$model",
                ],
            )

        return opt, val


class CLIParserExpectingValue(CLIParserState):
    def __init__(
        self,
        key_name: str,
        cfg_opts: t.Optional[t.Dict[str, t.Any]] = None,
        ctx_opts: t.Optional[t.Dict[str, t.Any]] = None,
        ctx_started: bool = False,
    ):
        super().__init__(cfg_opts, ctx_opts, ctx_started)
        self.key_name = key_name

    @property
    @abstractmethod
    def target_cfg(self) -> t.Dict[str, t.Any]:
        pass

    def process_token(self, token: str) -> CLIParserState:
        from pipelime.choixe.utils.common import deep_set_

        # the value is indeed a new key, so the previous key is a boolean flag
        if token in CLISpecialChars.ctx_start() or token.startswith(
            CLISpecialChars.config() + CLISpecialChars.context()
        ):
            self._set_boolean_flag()
            cli_state = CLIParserHoldState(
                None, self.cfg_opts, self.ctx_opts, self.ctx_started
            )
            return cli_state.process_token(token)

        deep_set_(
            self.target_cfg,
            key_path=self.key_name,
            value=self._convert_val(token),
            append=True,
        )
        return CLIParserHoldState(
            (self.key_name, self.target_cfg is self.cfg_opts),
            self.cfg_opts,
            self.ctx_opts,
            self.ctx_started,
        )

    def close(self):
        self._set_boolean_flag()

    def _set_boolean_flag(self):
        from pipelime.choixe.utils.common import deep_set_

        deep_set_(
            self.target_cfg,
            key_path=self.key_name,
            value=True,
            append=True,
        )

    def _convert_val(self, val: str):
        if val.lower() == "true":
            return True
        if val.lower() == "false":
            return False
        if val.lower() in ("none", "null", "nul"):
            return None
        try:
            num = int(val)
            return num
        except ValueError:
            pass
        try:
            num = float(val)
            return num
        except ValueError:
            pass
        if (val.startswith("'") and val.endswith("'")) or (
            val.startswith('"') and val.endswith('"')
        ):
            return val[1:-1]
        return val


class CLIParserExpectingCfgValue(CLIParserExpectingValue):
    @property
    def target_cfg(self) -> t.Dict[str, t.Any]:
        return self.cfg_opts


class CLIParserExpectingCtxValue(CLIParserExpectingValue):
    @property
    def target_cfg(self) -> t.Dict[str, t.Any]:
        return self.ctx_opts
