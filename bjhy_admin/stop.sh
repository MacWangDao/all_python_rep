#!/bin/sh
SIGNAL=${SIGNAL:-TERM}
PIDS=$(ps axu | grep "python manage.py runserver 192.168.101.218:8000 --noreload" | grep -v grep |awk '{print $2}')
if [ -z "$PIDS" ]; then
  echo "No  python manage.py runserver 192.168.101.218:8000"
  exit 1
else
  kill -s $SIGNAL $PIDS
fi