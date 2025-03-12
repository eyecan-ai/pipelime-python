from pydantic.v1 import BaseModel


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

    progress: int = 0
    """The progress of the current chunk"""

    finished: bool = False
    """Whether th operation has finished"""
