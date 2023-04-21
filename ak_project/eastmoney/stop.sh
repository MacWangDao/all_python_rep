#!/bin/sh
SIGNAL=${SIGNAL:-TERM}
PIDS=$(ps axu | grep "python aio_em_data.py" | grep -v grep |awk '{print $2}')
if [ -z "$PIDS" ]; then
  echo "No  aio_em_data to stop"
  exit 1
else
  kill -s $SIGNAL $PIDS
fi
