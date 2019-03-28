#!/usr/bin/env sh
RUNMODE="${1:-app}"

echo "Jticker-controller version: `cat /app/version.txt`; node: `hostname`"

if [ "${RUNMODE}" = "app" ]; then
    echo "Starting gunicorn..."
    gunicorn --bind ${LISTEN_HOST:-0.0.0.0}:${LISTEN_PORT:-8000} jticker_controller.app:make_app --access-logfile - --worker-class aiohttp.worker.GunicornWebWorker --reload -t 3600 -w ${WORKERS:-10}
elif [ "${RUNMODE}" = "test" ]; then
    echo "Starting tests..."
    pytest "${@:5}"
else
    echo "Executing management command"
    python manage.py "$@"
fi
