from pydantic import BaseModel
from abc import ABC, abstractmethod


class MyMetaclass(type):
    def __new__(cls, name, bases, attrs):
        print(name, bases, attrs)


class MyModel(BaseModel, metaclass=MyMetaclass):
    a: int = 10
    b: float = 0.02

    @abstractmethod
    def foo(self):
        pass
