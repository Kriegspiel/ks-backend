# ks-backend deployment runbook

Runtime:
- Repo path: /home/fil/dev/kriegspiel/ks-backend
- Service: ks-backend.service
- Bind: 127.0.0.1:8000
- Public hostnames: https://api.kriegspiel.org and https://app.kriegspiel.org/api/*

Deploy:
cd /home/fil/dev/kriegspiel/ks-backend
.venv/bin/pip install -r src/app/requirements-dev.txt
sudo systemctl restart ks-backend.service

Verify:
curl -fsS https://api.kriegspiel.org/health
curl -fsS https://app.kriegspiel.org/api/health
