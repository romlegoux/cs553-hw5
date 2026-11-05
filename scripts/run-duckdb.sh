#!/bin/bash
# run-duckdb.sh: DuckDB baseline (B1, B2) on the large VM, Parquet on local disk, Hadoop stopped
set -uo pipefail
RES=~/hw5/results/part3/duckdb; mkdir -p $RES
SQL=~/hw5/src/sql
CSV=$RES/duckdb.csv
[ -f $CSV ] || echo "stage,dataset,rep,seconds" > $CSV
D=/home/ubuntu/duck        # working dir on large

on_large() { lxc exec large -- sudo -H -u ubuntu bash -lc "$1"; }
drop_caches() {
  sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
  lxc exec large -- bash -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
}
# DuckDB list of the first n files
files() { local l=""; for i in $(seq 0 $(($1 - 1))); do l="$l'/home/ubuntu/fineweb/$(printf %03d $i).parquet',"; done; echo "[${l%,}]"; }

# timed <query> <dataset> <nb files> <rep>  -> prints secs
timed() {
  local q=$1 ds=$2 n=$3 rep=$4 out=$D/out/$2
  sed -e "s#@FILES@#$(files $n)#g" -e "s#@OUT@#$out#g" -e "s#@TMP@#$D/tmp#g" $SQL/$q.sql > $RES/${q}_$ds.sql
  lxc file push --uid 1000 --gid 1000 $RES/${q}_$ds.sql large$D/run.sql > /dev/null
  on_large "rm -rf $out/$q.* $D/tmp; mkdir -p $out $D/tmp"
  drop_caches
  on_large "cd $D && /usr/bin/time -f %e -o /tmp/time duckdb < run.sql" > $RES/${q}_${ds}_r$rep.log 2>&1
  local secs=$(on_large "tail -1 /tmp/time")
  echo "$q,$ds,$rep,$secs" >> $CSV
  echo "duckdb $q $ds r$rep: ${secs}s" >&2
  echo $secs
}
# 1 run, 2 more if < 10 min
run() {
  local s=$(timed $1 $2 $3 1); s=${s%.*}
  if [ -n "$s" ] && [ "$s" -lt 600 ]; then timed $1 $2 $3 2 > /dev/null; timed $1 $2 $3 3 > /dev/null; fi
}

for ds in ${DATASETS:-fw1 fw8 fw15}; do
  n=${ds#fw}
  run b1 $ds $n
  run b2 $ds $n
  # check (not timed): same results as MapReduce / Spark (md5 from cluster B agree.csv)
  b1=$(on_large "LC_ALL=C sort $D/out/$ds/b1.tsv | md5sum | cut -d' ' -f1")
  b2=$(on_large "cd $D && duckdb -noheader -list -c \"SELECT id FROM 'out/$ds/b2.parquet'\" | LC_ALL=C sort | md5sum | cut -d' ' -f1")
  r1=$(grep "^B1,$ds," ~/hw5/results/part3/clusterB/agree.csv | cut -d, -f3)
  r2=$(grep "^B2,$ds," ~/hw5/results/part3/clusterB/agree.csv | cut -d, -f3)
  echo "B1,$ds,$b1,$r1,$([ "$b1" = "$r1" ] && echo yes || echo no)" | tee -a $RES/agree_duckdb.csv
  echo "B2,$ds,$b2,$r2,$([ "$b2" = "$r2" ] && echo yes || echo no)" | tee -a $RES/agree_duckdb.csv
  on_large "rm -rf $D/out/$ds"
done
