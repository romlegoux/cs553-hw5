#!/bin/bash
# run-scaling.sh: 4e, B2 with N = 1, 2, 4, 8 workers
# strong: FW-2 for every N (FW-8 does not fit on one worker's disk)
# weak: first N files -> N=1 fw1, N=4 fw4 (N=2 = strong N=2, N=8 = B2 FW-8 from 4c)
# run on the node, inside tmux, never at the same time as another script
set -uo pipefail
VMS=$(lxc list --format csv -c n)
RES=~/hw5/results/part4e/clusterB; mkdir -p $RES
CSV=$RES/scaling_fw2.csv
[ -f $CSV ] || echo "mode,input,framework,workers,rep,seconds" > $CSV
JAR=/home/ubuntu/hw5-1.0.jar
P=edu.iit.cs553.hw5
YARN=/home/ubuntu/hadoop-3.5.0/bin/yarn

on_tiny() { lxc exec tiny -- sudo -H -u ubuntu bash -lc "$1"; }

drop_caches() {
  sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
  for vm in $VMS; do lxc exec "$vm" -- bash -c 'sync; echo 3 > /proc/sys/vm/drop_caches'; done
}

# keep NodeManagers on small1..N only, DataNodes untouched
set_workers() {
  for i in $(seq 1 8); do
    a=stop; [ $i -le $1 ] && a=start
    on_tiny "ssh -n small$i $YARN --daemon $a nodemanager" > /dev/null 2>&1
  done
  until [ "$(on_tiny 'yarn node -list 2>/dev/null | grep -c RUNNING')" = "$1" ]; do sleep 5; done
  echo "== $1 NodeManagers running ==" >&2
}
trap 'set_workers 8' EXIT   # always restore the full cluster

# timed_run <mode> <input> <framework> <N> <rep>  -> prints secs
timed_run() {
  local mode=$1 in=$2 fw=$3 n=$4 rep=$5
  local out=/pipe/scale/$fw cmd
  case $fw in
    mapreduce) cmd="hadoop jar $JAR $P.B2FilterDedupMR /fineweb/tsv/$in $out $R" ;;
    spark)     cmd="spark-submit --num-executors $E --class $P.B2FilterDedupSpark $JAR /fineweb/tsv/$in $out $R" ;;
  esac
  on_tiny "hdfs dfs -rm -r -f -skipTrash $out" > /dev/null 2>&1
  drop_caches
  on_tiny "/usr/bin/time -f %e -o /tmp/time $cmd" > $RES/${mode}_${in}_${fw}_N${n}_r$rep.log 2>&1
  local secs=$(on_tiny "tail -1 /tmp/time")
  on_tiny "hdfs dfs -rm -r -f -skipTrash $out" > /dev/null 2>&1   # free disk right away
  echo "$mode,$in,$fw,$n,$rep,$secs" >> $CSV
  echo "$mode $in $fw N=$n r$rep: ${secs}s" >&2
  echo $secs
}

# skip if already measured, else 1 run + 2 more if < 10 min
run_stage() {
  if grep -q "^$1,$2,$3,$4," $CSV; then echo "skip $1 $2 $3 N=$4 (done)" >&2; return; fi
  local secs=$(timed_run "$@" 1)
  local s=${secs%.*}
  if [ -n "$s" ] && [ "$s" -lt 600 ]; then
    timed_run "$@" 2 > /dev/null
    timed_run "$@" 3 > /dev/null
  fi
}

# inputs: first 1, 2 and 4 files (not timed, full cluster)
set_workers 8
for n in 1 2 4; do
  on_tiny "hdfs dfs -test -e /fineweb/tsv/fw$n || spark-submit --class $P.B0Ingest $JAR '/fineweb/raw/00[0-$((n-1))].parquet' /fineweb/tsv/fw$n" > $RES/b0_fw$n.log 2>&1
done

for N in ${NS:-8 4 2 1}; do
  set_workers $N
  R=$((7 * N - 1))   # ~1.7 waves of the 4N reduce slots (55 at N=8)
  E=$((2 * N))       # 2 executors per worker (16 at N=8)
  for fw in mapreduce spark; do
    run_stage strong fw2 $fw $N
    case $N in 1|4) run_stage weak fw$N $fw $N ;; esac
  done
done
