from pydantic import BaseModel


class OperationInfo(BaseModel, frozen=True):
    """Information on a running operation."""

    token: str
    """The piper token identifying the session"""

    node: str
    """The node currently being executed"""

    chunk: int
    """The current chunk number"""

    message: str = ""
    """A short description of the current chunk"""

    total: int
    """The total number of steps in the current chunk"""


class ProgressUpdate(BaseModel):
    """Advacncement of an operation."""

    op_info: OperationInfo
    """The operation info"""

    just_started: bool = False
    """True if this operation has just started"""

    advance: int = 1
    """The progress of the current chunk w.r.t. the previous update"""

    finished: bool = False
    """True if this operation has finished"""
