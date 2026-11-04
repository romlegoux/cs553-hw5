#!/bin/bash
# run-q8.sh: Q8, B1 and B4 on FW-8 reading the Parquet files instead of the TSV
set -uo pipefail
VMS=$(lxc list --format csv -c n)
RES=~/hw5/results/questions/q8; mkdir -p $RES
JAR=/home/ubuntu/hw5-1.0.jar
P=edu.iit.cs553.hw5
IN="/fineweb/raw/00[0-7].parquet"

on_tiny() { lxc exec tiny -- sudo -H -u ubuntu bash -lc "$1"; }
drop_caches() {
  sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
  for vm in $VMS; do lxc exec "$vm" -- bash -c 'sync; echo 3 > /proc/sys/vm/drop_caches'; done
}

# timed <name> <output dir> <command>  -> prints secs
timed() {
  on_tiny "hdfs dfs -rm -r -f -skipTrash $2" > /dev/null 2>&1
  drop_caches
  on_tiny "/usr/bin/time -f %e -o /tmp/time $3" > $RES/$1.log 2>&1
  local secs=$(on_tiny "tail -1 /tmp/time")
  echo "$1,$secs" | tee -a $RES/q8.csv >&2
  echo $secs
}
# 1 run, 2 more if < 10 min
repeat() {
  local s=$(timed "$1_r1" "$2" "$3"); s=${s%.*}
  if [ -n "$s" ] && [ "$s" -lt 600 ]; then
    timed "$1_r2" "$2" "$3" > /dev/null
    timed "$1_r3" "$2" "$3" > /dev/null
  fi
}

repeat b1_parquet /q8/b1 "spark-submit --class $P.B1WordCountSparkParquet $JAR '$IN' /q8/b1 55"
repeat b4_parquet /q8/b4 "spark-submit --class $P.B4Report $JAR '$IN' /pipe/b2mr/fw8 /pipe/b3/fw8 /q8/b4"

# same results as the TSV versions
on_tiny "for d in /q8/b1 /pipe/b1mr/fw8; do hdfs dfs -cat \$d/part-* | LC_ALL=C sort | md5sum; done" | tee $RES/q8_b1_agree.txt
on_tiny "hdfs dfs -cat /q8/b4/funnel/part-*" > $RES/funnel_fw8_parquet.csv
diff $RES/funnel_fw8_parquet.csv ~/hw5/results/part3/clusterB/funnel_fw8.csv && echo "B4 funnel identical" | tee $RES/q8_b4_agree.txt
