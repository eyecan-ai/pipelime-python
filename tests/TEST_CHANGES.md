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
