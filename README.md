# Backend

## Layout

- `src/app`: active FastAPI application and API wiring.
- `src/tests`: active pytest suite (configured by `backend/pyproject.toml`).
- `scripts`: ad hoc backend validation helpers.

## Legacy cleanup

Issue #12 removed the last unused pre-2026 backend leftovers:

- deleted `backend/models.py`
- deleted `backend/kriegspiel_wrapper.py`
- removed the unused legacy `/games` routes and SQLite bootstrap from `src/app/main.py`

The supported backend runtime now lives entirely under `backend/src/app` and `backend/src/tests`.
