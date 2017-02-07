#! /bin/bash
echo "killing dask instances"
#killall dask-scheduler
killall dask-worker


if (($1 == 0)); then
  echo "Starting no workers and no scheduler"
  exit 0
fi 

echo "starting $1 workers with scheduler at $2"

#nohup dask-scheduler &
for i in `seq 1 $1`
do
  sleep 0.5s
  echo "starting worker"
  nohup dask-worker $2 --nprocs 1 --nthreads 2 &
done

echo "all workers started"
