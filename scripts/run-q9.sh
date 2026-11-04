#!/bin/bash
# run-q9.sh: Q9, B2 on FW-8 (MR then Spark), stop one worker's NodeManager halfway
set -uo pipefail
VMS=$(lxc list --format csv -c n)
RES=~/hw5/results/questions/q9; mkdir -p $RES
PIPE=~/hw5/results/part3/clusterB/pipeline.csv
JAR=/home/ubuntu/hw5-1.0.jar
P=edu.iit.cs553.hw5
YARN=/home/ubuntu/hadoop-3.5.0/bin/yarn
[ -f $RES/q9.csv ] || echo "framework,seconds,normal_mean,victim,am_host,output_ok" > $RES/q9.csv

on_tiny() { lxc exec tiny -- sudo -H -u ubuntu bash -lc "$1"; }
drop_caches() {
  sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
  for vm in $VMS; do lxc exec "$vm" -- bash -c 'sync; echo 3 > /proc/sys/vm/drop_caches'; done
}
wait_nodes() { until [ "$(on_tiny 'yarn node -list 2>/dev/null | grep -c RUNNING')" = "$1" ]; do sleep 5; done; }

# ids of the reference output (B2 MR on FW-8, no failure)
REF=$(on_tiny "hdfs dfs -cat /pipe/b2mr/fw8/part-* | cut -f1 | LC_ALL=C sort | md5sum | cut -d' ' -f1")

# q9 <framework> <command>
q9() {
  local fw=$1 cmd=$2 out=/q9/$1
  local mean=$(awk -F, -v f=$fw '$1=="B2" && $2==f && $3=="fw8" {s+=$5; n++} END {printf "%.0f", s/n}' $PIPE)
  on_tiny "hdfs dfs -rm -r -f -skipTrash $out" > /dev/null 2>&1
  wait_nodes 8; drop_caches

  # run in background, kill a NodeManager at half the normal time
  on_tiny "/usr/bin/time -f %e -o /tmp/time_q9 $cmd $out 55" > $RES/b2_${fw}_failure.log 2>&1 &
  local pid=$!
  sleep $((mean / 2))
  local app=$(on_tiny "yarn application -list 2>/dev/null" | grep -om1 'application_[0-9_]*')
  local am=$(on_tiny "yarn application -status $app 2>/dev/null" | awk -F' : ' '/AM Host/ {print $2}' | tr -d ' ')
  local victim=small8; [ "$am" = small8 ] && victim=small7
  echo "$(date -u +%T) $fw: stop NodeManager on $victim (app $app, AM on $am, normal ${mean}s)" | tee -a $RES/q9_events.txt
  on_tiny "ssh -n $victim $YARN --daemon stop nodemanager"

  wait $pid
  local secs=$(on_tiny "tail -1 /tmp/time_q9")
  echo "$(date -u +%T) $fw: finished in ${secs}s" | tee -a $RES/q9_events.txt
  on_tiny "ssh -n $victim $YARN --daemon start nodemanager"

  local got=$(on_tiny "hdfs dfs -cat $out/part-* | cut -f1 | LC_ALL=C sort | md5sum | cut -d' ' -f1")
  local ok=no; [ -n "$got" ] && [ "$got" = "$REF" ] && ok=yes
  echo "$fw,$secs,$mean,$victim,$am,$ok" | tee -a $RES/q9.csv
  on_tiny "hdfs dfs -rm -r -f -skipTrash $out" > /dev/null 2>&1
}

q9 mapreduce "hadoop jar $JAR $P.B2FilterDedupMR /fineweb/tsv/fw8"
q9 spark     "spark-submit --class $P.B2FilterDedupSpark $JAR /fineweb/tsv/fw8"
wait_nodes 8
