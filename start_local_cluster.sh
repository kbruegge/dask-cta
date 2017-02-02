#! /bin/bash
echo "killing dask instances"
killall dask-scheduler
killall dask-worker


if (($1 == 0)); then
  echo "Starting no workers and no scheduler"
  exit 0
fi 

echo "starting $1 workers"

nohup dask-scheduler &
for i in `seq 1 $1`
do
  sleep 1s
  echo "starting worker"
  nohup dask-worker 127.0.0.1:8786 --nprocs 1 --nthreads 1 &
done

echo "all workers started"
