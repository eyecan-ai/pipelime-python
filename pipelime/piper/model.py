from typing import Dict, Optional, Sequence

from pydantic import BaseModel, Field


class NodeModel(BaseModel):
    command: str
    args: Optional[dict] = None
    inputs: Optional[dict] = None
    outputs: Optional[dict] = None
    outputs_schema: Optional[dict] = None
    inputs_schema: Optional[dict] = None

    def get_output_schema(self, name: str) -> Optional[str]:
        if self.outputs_schema is not None:
            return self.outputs_schema.get(name, None)
        return None

    def get_input_schema(self, name: str) -> Optional[str]:
        if self.inputs_schema is not None:
            return self.inputs_schema.get(name, None)
        return None


class DAGModel(BaseModel):
    nodes: Dict[str, NodeModel]

    def purged_dict(self):
        return self.dict(
            exclude_unset=True,
            exclude_none=True,
        )


class PiperModel(BaseModel):
    token: str = ""
    node: str = ""
    inputs: Sequence[str] = Field(default_factory=list)
    outputs: Sequence[str] = Field(default_factory=list)

    @property
    def active(self) -> bool:
        return len(self.token) > 0
