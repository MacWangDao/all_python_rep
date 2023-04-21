#!/bin/sh
SIGNAL=${SIGNAL:-TERM}
PIDS=$(ps axu | grep "python fast_api_main.py" | grep -v grep |awk '{print $2}')
if [ -z "$PIDS" ]; then
  echo "No  fast_api_main to stop"
  exit 1
else
  kill -s $SIGNAL $PIDS
fi