from typing import ContextManager


class ContextManagerList:
    def __init__(self, *args: ContextManager):
        self._cms = args

    def __enter__(self):
        for cm in self._cms:
            cm.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        for cm in self._cms:
            cm.__exit__(exc_type, exc_value, traceback)
