#!/bin/sh
SIGNAL=${SIGNAL:-TERM}
PIDS=$(ps axu | grep "ths_index_industry.py" | grep -v grep |awk '{print $2}')
if [ -z "$PIDS" ]; then
  echo "No  ths_index_industry.py to stop"
  exit 1
else
  kill -s $SIGNAL $PIDS
fi
