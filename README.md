# ks-backend

Backend repository for api.kriegspiel.org and the /api surface behind app.kriegspiel.org.

Scope:
- FastAPI application under src/app
- Backend tests under src/tests
- Backend utilities under scripts/

Local development:
python3.15 -m venv .venv
. .venv/bin/activate
pip install -r src/app/requirements-dev.txt
uvicorn app.main:app --app-dir src --reload --port 8000

Test:
./scripts/regression/backend-regression.sh

Python runtime: CPython 3.15.2, pinned in `.python-version`. Keep the OS Python unchanged; production runtime migration uses `ks-deploy python-runtime` (see the platform migration runbook).

Container validation (run from the repository root):

```bash
docker build -f src/app/Dockerfile -t ks-backend:local .
```

The image uses CPython 3.15.2. Supply `MONGO_URI` and runtime secrets through the deployment environment. Root `.env` files are excluded from the build context.
