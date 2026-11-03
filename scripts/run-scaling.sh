#!/bin/bash
# run-scaling.sh: 4e, B2 with N = 1, 2, 4 workers (N = 8 comes from 4c)
# run on the node, inside tmux, AFTER run-pipeline.sh (never both at once)
set -uo pipefail
VMS=$(lxc list --format csv -c n)
RES=~/hw5/results/part4e/clusterB; mkdir -p $RES
CSV=$RES/scaling.csv
[ -f $CSV ] || echo "mode,framework,workers,rep,seconds" > $CSV
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

# timed_run <mode> <framework> <N> <output dir> <command> <rep>
timed_run() {
  local mode=$1 fw=$2 n=$3 out=$4 cmd=$5 rep=$6
  on_tiny "hdfs dfs -rm -r -f -skipTrash $out" > /dev/null 2>&1
  drop_caches
  on_tiny "/usr/bin/time -f %e -o /tmp/time $cmd" > $RES/${mode}_${fw}_N${n}_r$rep.log 2>&1
  local secs=$(on_tiny "tail -1 /tmp/time")
  echo "$mode,$fw,$n,$rep,$secs" >> $CSV
  echo "$mode $fw N=$n r$rep: ${secs}s" >&2
  echo $secs
}

# 1 run, 2 more if < 10 min
run_stage() {
  local secs=$(timed_run "$@" 1)
  local s=${secs%.*}
  if [ -n "$s" ] && [ "$s" -lt 600 ]; then
    timed_run "$@" 2 > /dev/null
    timed_run "$@" 3 > /dev/null
  fi
}

# weak scaling inputs: first 2 and first 4 files (not timed, full cluster)
set_workers 8
for n in 2 4; do
  on_tiny "hdfs dfs -test -e /fineweb/tsv/fw$n || spark-submit --class $P.B0Ingest $JAR '/fineweb/raw/00[0-$((n-1))].parquet' /fineweb/tsv/fw$n" > $RES/b0_fw$n.log 2>&1
done

for N in ${NS:-4 2 1}; do
  set_workers $N
  R=$((7 * N - 1))   # 7 containers per worker - 1 AM (55 at N=8)
  E=$((2 * N))       # 2 executors per worker (16 at N=8)

  run_stage strong mapreduce $N /pipe/scale/mr    "hadoop jar $JAR $P.B2FilterDedupMR /fineweb/tsv/fw8 /pipe/scale/mr $R"
  run_stage strong spark     $N /pipe/scale/spark "spark-submit --num-executors $E --class $P.B2FilterDedupSpark $JAR /fineweb/tsv/fw8 /pipe/scale/spark $R"
  run_stage weak   mapreduce $N /pipe/scale/mr    "hadoop jar $JAR $P.B2FilterDedupMR /fineweb/tsv/fw$N /pipe/scale/mr $R"
  run_stage weak   spark     $N /pipe/scale/spark "spark-submit --num-executors $E --class $P.B2FilterDedupSpark $JAR /fineweb/tsv/fw$N /pipe/scale/spark $R"
done
