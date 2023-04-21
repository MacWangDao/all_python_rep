#!/bin/sh
SIGNAL=${SIGNAL:-TERM}
PIDS=$(ps axu | grep "python aps_main.py" | grep -v grep |awk '{print $2}')
if [ -z "$PIDS" ]; then
  echo "No  server to stop"
  exit 1
else
  kill -s $SIGNAL $PIDS
fi

for i in `ps axu | grep chromium | awk '{print $2}'`;do kill $i; done
for i in `ps axu | grep chromedriver | awk '{print $2}'`;do kill $i; done