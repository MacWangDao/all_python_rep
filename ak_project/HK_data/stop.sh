#!/bin/sh
SIGNAL=${SIGNAL:-TERM}
PIDS=$(ps axu | grep "python hkexnews_http_client_data_new.py" | grep -v grep |awk '{print $2}')
if [ -z "$PIDS" ]; then
  echo "No  hkexnews_http_client_data_new to stop"
  exit 1
else
  kill -s $SIGNAL $PIDS
fi
