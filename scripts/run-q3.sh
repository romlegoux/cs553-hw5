#!/bin/bash
# run-q3.sh: Q3, B1 on FW-1 without combiner (MR) and with groupByKey (Spark)
set -uo pipefail
VMS=$(lxc list --format csv -c n)
RES=~/hw5/results/questions/q3; mkdir -p $RES
JAR=/home/ubuntu/hw5-1.0.jar
P=edu.iit.cs553.hw5

on_tiny() { lxc exec tiny -- sudo -H -u ubuntu bash -lc "$1"; }
drop_caches() {
  sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
  for vm in $VMS; do lxc exec "$vm" -- bash -c 'sync; echo 3 > /proc/sys/vm/drop_caches'; done
}

# run <name> <output dir> <command>
run() {
  on_tiny "hdfs dfs -rm -r -f -skipTrash $2" > /dev/null 2>&1
  drop_caches
  on_tiny "/usr/bin/time -f %e -o /tmp/time $3" > $RES/$1.log 2>&1
  echo "$1,$(on_tiny 'tail -1 /tmp/time')" | tee -a $RES/q3.csv
}

for r in 1 2 3; do
  run mr_nocombiner_r$r /q3/mr "hadoop jar $JAR $P.B1WordCountMR -Dwc.combiner=false /fineweb/tsv/fw1 /q3/mr 55"
done
run spark_groupbykey_r1 /q3/spark "spark-submit --class $P.B1WordCountSpark $JAR /fineweb/tsv/fw1 /q3/spark 55 group"

# same result as B1 with combiner / reduceByKey (3 identical md5)
on_tiny "for d in /q3/mr /q3/spark /pipe/b1mr/fw1; do hdfs dfs -cat \$d/part-* | LC_ALL=C sort | md5sum; done" | tee $RES/q3_agree.txt
on_tiny "hdfs dfs -rm -r -f -skipTrash /q3" > /dev/null 2>&1
