#!/bin/sh
SIGNAL=${SIGNAL:-TERM}
PIDS=$(ps axu | grep "python img_excel_aps_start.py" | grep -v grep |awk '{print $2}')
if [ -z "$PIDS" ]; then
  echo "No  img_excel_aps_start to stop"
  exit 1
else
  kill -s $SIGNAL $PIDS
fi