#!/bin/bash
alembic upgrade head &
python worker_payment.py &
python worker_webhook.py &
python worker_reconciliation.py &
uvicorn main:app --host 0.0.0.0 --port $PORT