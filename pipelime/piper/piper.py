from __future__ import annotations

import functools
import io
import subprocess
import uuid
from typing import Any, Callable, Dict, Optional, Sequence, Union

import click
import yaml
from loguru import logger
from yaml.scanner import ScannerError


class PiperNamespace:
    """Namespace constants for Piper ecosystem."""

    PIPER_PREFIX = "piper_"
    PRIVATE_ARGUMENT_PREFIX = "---"
    PRIVATE_OPTION_PREFIX = "_"
    NAME_INPUTS = "inputs"
    NAME_OUTPUTS = "outputs"
    NAME_TOKEN = "token"
    NAME_INFO = "info"
    NAME_ARGS = "args"
    COMMAND_KWARGS_NAME = "piper_kwargs"

    """ Names of kwargs variable """
    OPTION_NAME_INPUTS = f"{PRIVATE_OPTION_PREFIX}{PIPER_PREFIX}{NAME_INPUTS}"
    OPTION_NAME_OUTPUTS = f"{PRIVATE_OPTION_PREFIX}{PIPER_PREFIX}{NAME_OUTPUTS}"
    OPTION_NAME_TOKEN = f"{PRIVATE_OPTION_PREFIX}{PIPER_PREFIX}{NAME_TOKEN}"
    OPTION_NAME_INFO = f"{PRIVATE_OPTION_PREFIX}{PIPER_PREFIX}{NAME_INFO}"

    """ Names of click arguments """
    ARGUMENT_NAME_INPUTS = f"{PRIVATE_ARGUMENT_PREFIX}{PIPER_PREFIX}{NAME_INPUTS}"
    ARGUMENT_NAME_OUTPUTS = f"{PRIVATE_ARGUMENT_PREFIX}{PIPER_PREFIX}{NAME_OUTPUTS}"
    ARGUMENT_NAME_TOKEN = f"{PRIVATE_ARGUMENT_PREFIX}{PIPER_PREFIX}{NAME_TOKEN}"
    ARGUMENT_NAME_INFO = f"{PRIVATE_ARGUMENT_PREFIX}{PIPER_PREFIX}{NAME_INFO}"


class PiperCommand:
    instance: PiperCommand = None

    def __init__(
        self,
        fn: Callable,
        inputs: Sequence[str],
        outputs: Sequence[str],
    ) -> None:
        # Extract default values from the kwargs
        self._fn = fn
        self.__name__ = fn.__name__
        self._caller_name = f"{fn.__module__}:{fn.__name__}"
        self._inputs = inputs
        self._outputs = outputs

        # progress callbacks list
        self._progress_callbacks = []

    def _progress_callback(self, chunk_index: int, total_chunks: int, payload: dict):
        self.log(
            "_progress",
            {
                "chunk_index": chunk_index,
                "total_chunks": total_chunks,
                "progress_data": payload,
            },
        )

    def generate_progress_callback(
        self,
        chunk_index: int = 0,
        total_chunks: int = 1,
    ) -> Callable[[dict], None]:
        """Generates a progress callback function to send back to the caller. The callback
        should be called every time the external progress is updated. Internally the callback
        will log the progress (also in the communication channel).

        Args:
            chunk_index (int, optional): index for multiple user tasks. Defaults to 0.
            total_chunks (int, optional): total number of chunks. Defaults to 1.

        Returns:
            Callable[[dict], None]: the callback function
        """
        callback = functools.partial(self._progress_callback, chunk_index, total_chunks)
        self._progress_callbacks.append(callback)
        return callback

    def clear_progress_callbacks(self):
        self._progress_callbacks = []

    @property
    def active(self) -> bool:
        return self._active

    @property
    def _log_header(self) -> str:
        return f"{self._id}|"

    def log(self, key: str, value: any):
        """Logs a key/value pair into the communication channel.

        Args:
            key (str): The key to log.
            value (any): The value to log. Can be any picklable object.
        """
        if self.active:
            logger.debug(f"{self._log_header}Logging {key}={value}")
            # self._channel.send(self._id, {key: value})

    def _init_state(self, token: str) -> None:
        self.__class__.instance = self
        self._token = token if len(token) > 0 else None

        # Token check, if not provided, the command is disabled
        self._active = self._token is not None

        if self._active:
            # Builds an unique id for the command, multiple instances of same command
            # are allowed, for this reason an unique id is required
            self._unique_identifier = str(uuid.uuid1())
            self._id = f"{self._caller_name}:{self._unique_identifier}"

    def _log_state(self) -> None:
        if self._active:
            # Logs the command creation
            logger.debug(f"{self._log_header}New Piper created from: {self._id}")
            logger.debug(f"{self._log_header}\tPiper inputs: {self._inputs}")
            logger.debug(f"{self._log_header}\tPiper outputs: {self._outputs}")
            logger.debug(f"{self._log_header}\tPiper token: {self._token}")

    def _filter_kwargs(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        to_remove = {
            PiperNamespace.OPTION_NAME_TOKEN,
            PiperNamespace.OPTION_NAME_INFO,
            PiperNamespace.OPTION_NAME_OUTPUTS,
            PiperNamespace.OPTION_NAME_INPUTS,
        }
        return {k: v for k, v in kwargs.items() if k not in to_remove}

    def __call__(self, *args, **kwargs):
        token = kwargs.get(PiperNamespace.OPTION_NAME_TOKEN, "")
        kwargs = self._filter_kwargs(kwargs)
        self._init_state(token)
        self._log_state()
        self._fn(*args, **kwargs)
        self.__class__.instance = None


class Piper:
    @staticmethod
    def _piper_info_callback(
        ctx: click.core.Context, param: click.core.Option, value: bool
    ):
        """Callback for the click eager corresponding option. If the eager option is
        set to True, the command will exit printing the Piper info into the pipe.

        Args:
            ctx (click.core.Context): The click context.
            param (click.core.Option): The click option.
            value (bool): The value of the eager option.
        """
        if value:
            stream = io.StringIO()
            yaml.safe_dump(ctx.command.to_info_dict(ctx), stream)
            click.echo(stream.getvalue())  # (ctx.command.to_info_dict(ctx))
            ctx.exit()

    @staticmethod
    def command(
        inputs: Optional[Sequence[str]] = None,
        outputs: Optional[Sequence[str]] = None,
    ):
        """This is the special decorator for the Piper command. It is used to add hidden
        options to the command used to manager the Piper ecosystem.

        Args:
            inputs (Optional[Sequence[str]], optional): List of click command options
            treated as inputs. Defaults to None.
            outputs (Optional[Sequence[str]], optional): List of click command options
            treated as output . Defaults to None.
        """

        def _wrapped(func):
            cmd = PiperCommand(func, inputs, outputs)
            cmd = click.option(
                PiperNamespace.ARGUMENT_NAME_TOKEN, default="", hidden=True
            )(cmd)
            cmd = click.option(
                PiperNamespace.ARGUMENT_NAME_INPUTS, default=inputs, hidden=True
            )(cmd)
            cmd = click.option(
                PiperNamespace.ARGUMENT_NAME_OUTPUTS, default=outputs, hidden=True
            )(cmd)
            cmd = click.option(
                PiperNamespace.ARGUMENT_NAME_INFO,
                is_flag=True,
                is_eager=True,
                expose_value=False,
                callback=Piper._piper_info_callback,
                hidden=True,
            )(cmd)
            return cmd

        return _wrapped

    @classmethod
    def piper_command_raw_info(cls, command: str) -> Union[None, dict]:
        """Retrieves the Piper info from a generic bash command. If the bash command
        is a valid Piper command, the Piper info will be returned as a dict. If the
        bash command is not a valid Piper command, None will be returned.

        Args:
            command (str): The bash command to execute. The command should be the raw bash
            command, without any arguments provided. Automatically the eager option will
            be added to the command to force the Piper to manifest itself.

        Returns:
            Union[None, dict]: The Piper info as a dict or None if the bash command
        """

        # Append the piper eager option to the command
        command += f" {PiperNamespace.ARGUMENT_NAME_INFO}"
        info = None

        pipe = subprocess.Popen(
            command.split(" "),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = pipe.communicate()
        if pipe.returncode == 0:
            try:
                info = yaml.safe_load(stdout)
            except ScannerError:
                logger.error(f"{command} is not a valid Piper command!")
                info = None
        else:
            raise TypeError(stderr)

        return info

    @classmethod
    def piper_command_description(cls, command: str) -> dict:
        """Retrieves the Piper structured description from a generic bash command.
        If the bash command is a valid Piper command, the Piper description will be returned
        as a dict. If the bash command is not a valid Piper command, None will be returned.

        Args:
            command (str): The bash command to execute. The command should be the raw bash
            command, without any arguments provided. Automatically the eager option will
            be added to the command to force the Piper to manifest itself.

        Raises:
            RuntimeError: If the Piper command is not a valid Piper command.

        Returns:
            dict: The Piper description as a dict or None if the bash command is not a
            valid Piper command.
        """

        raw_info = cls.piper_command_raw_info(command)
        if raw_info is None:
            raise TypeError(f"Command '{command}' is not a piper!")

        commands_map = {x["name"]: x for x in raw_info["params"]}

        piper_inputs = commands_map[PiperNamespace.OPTION_NAME_INPUTS]
        piper_outputs = commands_map[PiperNamespace.OPTION_NAME_OUTPUTS]

        # Remove the piper options from the commands map
        del commands_map[PiperNamespace.OPTION_NAME_INPUTS]
        del commands_map[PiperNamespace.OPTION_NAME_OUTPUTS]
        del commands_map[PiperNamespace.OPTION_NAME_TOKEN]
        del commands_map[PiperNamespace.OPTION_NAME_INFO]

        # Retrieves inputs/outputs fields
        inputs_list = piper_inputs["default"]
        outputs_list = piper_outputs["default"]

        # initialize the description
        description = {
            PiperNamespace.NAME_INPUTS: {},
            PiperNamespace.NAME_OUTPUTS: {},
            PiperNamespace.NAME_ARGS: {},
        }

        # For each inputs check if a corresponding click command option is present
        if inputs_list is not None:
            for i in inputs_list:
                if i not in commands_map:
                    raise KeyError(f"Input '{i}' is not a valid Piper command option!")
                description[PiperNamespace.NAME_INPUTS][i] = commands_map[i]
                del commands_map[i]

        # For each outputs check if a corresponding click command option is present
        if outputs_list is not None:
            for o in outputs_list:
                if o not in commands_map:
                    raise KeyError(f"Output '{o}' is not a valid Piper command option!")
                description[PiperNamespace.NAME_OUTPUTS][o] = commands_map[o]
                del commands_map[o]

        # Adds remaining options as generic arguments
        description[PiperNamespace.NAME_ARGS] = commands_map

        return description

    @classmethod
    def piper_info_argument(cls):
        return PiperNamespace.ARGUMENT_NAME_INFO

    @classmethod
    def piper_token_argument(cls):
        return PiperNamespace.ARGUMENT_NAME_TOKEN
