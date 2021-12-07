#!/usr/bin/env bash
export BROKER_USER='guest'
export BROKER_PASS='guest'
export BROKER_HOST='localhost'
export BROKER_PORT=5672
echo -e "*******************************************************************\n"

export PYTHONPATH=.:..:../..:../../..:$PYTHONPATH;
celery -A demo_task worker