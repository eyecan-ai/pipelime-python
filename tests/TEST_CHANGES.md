# Test changes during the pydantic v1 → v2 migration

Rule (design spec §5.1): a test may be edited only when it uses a pydantic **v1 API
form** in test-local code — never to accommodate a behaviour change. Every edit is
listed here.

| Subtask | File | Change | Why (v1 form replaced) |
|---|---|---|---|
| S0 | `tests/conftest.py` | xdist grouping hook | new infrastructure, no behaviour |
| S0 | `test_receiver_zmq.py`, `test_tracker_zmq.py` | `xdist_group("zmq")` marker | new infrastructure, no behaviour |
