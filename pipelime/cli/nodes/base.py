import pydantic as pyd
from abc import ABC, abstractmethod


class PipelimeCommand(pyd.BaseModel):
    @classmethod
    def get_node_name(cls) -> str:
        if cls.__config__.title:
            return cls.__config__.title
        return cls.__name__

    @classmethod
    def pretty_schema(
        cls, *, show_name: bool = True, indent: int = 0, indent_offs: int = 2
    ) -> str:
        import inspect

        schema_str = f"'{inspect.getdoc(cls)}' " + "{\n"
        if show_name:
            schema_str = ((" " * indent) + f"{cls.get_node_name()}: ") + schema_str

        for field in cls.__fields__.values():
            fname = field.name if not field.alias else field.alias

            if isinstance(field.type_, PipelimeCommand):
                fvalue = field.type_.pretty_schema(
                    show_name=False,
                    indent=indent_offs + indent,
                    indent_offs=indent_offs,
                )
            else:
                fhelp = (
                    f"'{field.field_info.description}' "
                    if field.field_info.description
                    else ""
                )
                fvalue = (
                    f"`{field.type_.__name__}`  "
                    + fhelp
                    + ("[required" if field.required else " [optional")
                    + f", default={field.get_default()}]"
                )
            schema_str += (" " * (indent_offs + indent)) + f"{fname}: {fvalue}\n"
        schema_str += (" " * indent) + "}"
        return schema_str


class PiperNode(PipelimeCommand, ABC):
    def __call__(self):
        self.run()

    @abstractmethod
    def run(self):
        pass
