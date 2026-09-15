# Test changes during the pydantic v1 → v2 migration

Rule (design spec §5.1): a test may be edited only when it uses a pydantic **v1 API
form** in test-local code — never to accommodate a behaviour change. Every edit is
listed here.

| Subtask | File | Change | Why (v1 form replaced) |
|---|---|---|---|
| S0 | `tests/conftest.py` | xdist grouping hook | new infrastructure, no behaviour |
| S0 | `test_receiver_zmq.py`, `test_tracker_zmq.py` | `xdist_group("zmq")` marker | new infrastructure, no behaviour |
| S1-T5 | `test_pydantic_types.py` | `import pydantic.v1 as pyd` → `import pydantic as pyd` | v1 shim import |
| S1-T6 | `test_pydantic_types.py` (`TestNumpyType`, `TestYamlInput`) | `pyd.parse_raw_as(plt.X, x.json())` → `plt.X.model_validate_json(x.model_dump_json())`; `pyd.parse_obj_as(plt.X, x.dict()["__root__"])` → `plt.X.model_validate(x.dict()["__root__"])` | `parse_raw_as`/`parse_obj_as`/`.json()` are v1-only APIs |
| S1-T6 | `test_pydantic_types.py` (`TestItemType`, `TestCallableDef`) | same `parse_raw_as`/`parse_obj_as` → `model_validate_json`/`model_validate` replacement, applied ahead of the T7 class conversion | `parse_raw_as`/`parse_obj_as`/`.json()` are v1-only APIs |
| S1-T7 | `test_pydantic_types.py` | added `import typing as t` and `TestCallableDef.test_string_annotations_resolved` (new regression test, not a v1-form edit) | forward-ref annotation resolution added to `CallableDef.args_type`/`return_type` |
| S1 | `tests/pipelime/utils/test_pydantic_compat.py` | the `PydanticUserError` pin moved from `Field("x", regex="x")` to `Field("x", const=True)`; new `regex=`/`pattern=` test | `regex=` is now translated to v2 `pattern=` by the pipelime `Field` wrapper, so it no longer raises |
| S2b-T8 | `tests/pipelime/choixe/visitors/test_decoder.py`, `test_processor.py` | `from pydantic.v1 import BaseModel` → `from pydantic import BaseModel` | v1 shim import (`$model`/`decode` now require v2 models) |
| S2b-T8 | `tests/pipelime/choixe/visitors/test_processor.py::TestProcessor::test_model` | `assert process(...) == expected` → compare `type(m).__name__` and `model_dump()` of result and expected | the `$model` symbol is imported from the test *file* (a second module object), so the classes differ; the equality relied on v1 `BaseModel.__eq__` comparing `.dict()` across classes (v2 requires the same class) |
