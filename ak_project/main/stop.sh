#!/bin/sh
SIGNAL=${SIGNAL:-TERM}
PIDS=$(ps axu | grep "python aio_ths_industry_rank_grade_v2.py" | grep -v grep |awk '{print $2}')
if [ -z "$PIDS" ]; then
  echo "No  aio_ths_industry_rank_grade_v2.py to stop"
  exit 1
else
  kill -s $SIGNAL $PIDS
fi
