#!/bin/sh
SIGNAL=${SIGNAL:-TERM}
PIDS=$(ps axu | grep "python news_v4.py" | grep -v grep |awk '{print $2}')
if [ -z "$PIDS" ]; then
  echo "No news_v4 to stop"
  exit 1
else
  kill -s $SIGNAL $PIDS
fi
