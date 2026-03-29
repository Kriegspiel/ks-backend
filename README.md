# ks-backend

Backend repository for api.kriegspiel.org and the /api surface behind app.kriegspiel.org.

Scope:
- FastAPI application under src/app
- Backend tests under src/tests
- Backend utilities under scripts/

Local development:
python3 -m venv .venv
. .venv/bin/activate
pip install -r src/app/requirements-dev.txt
uvicorn app.main:app --app-dir src --reload --port 8000

Test:
./scripts/regression/backend-regression.sh
