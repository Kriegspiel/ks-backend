# Backend

## Layout

- `src/app`: active FastAPI application and API wiring.
- `src/tests`: active pytest suite (configured by `backend/pyproject.toml`).
- `models.py` and `kriegspiel_wrapper.py`: legacy gameplay/storage adapters still used by `src/app/main.py`.

## 2025 File Audit

As part of issue #6, we removed files that were no longer referenced by the runtime or test configuration:

- `backend/main.py`
- `backend/test_main.py`
- `backend/test_database.py`

These files were superseded by the `src/app` and `src/tests` structure.
