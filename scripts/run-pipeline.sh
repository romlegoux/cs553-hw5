#!/bin/bash
# run-pipeline.sh: run on the node, inside tmux
# DATASETS="fw1" STAGES="B2spark B3 B4" ./run-pipeline.sh  to run only some datasets/stages
set -uo pipefail
VMS=$(lxc list --format csv -c n)
RES=~/hw5/results/part3/clusterB; mkdir -p $RES
CSV=$RES/pipeline.csv
[ -f $CSV ] || echo "stage,framework,dataset,rep,seconds" > $CSV
JAR=/home/ubuntu/hw5-1.0.jar
P=edu.iit.cs553.hw5
B3=B3NearDup   # B3 class name
R=55           # reducers / partitions, same as part 2
STAGES=${STAGES:-B0 B1mr B1spark B2mr B2spark B3 B4}

want() { [[ " $STAGES " == *" $1 "* ]]; }

on_tiny() { lxc exec tiny -- sudo -H -u ubuntu bash -lc "$1"; }

drop_caches() {
  sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
  for vm in $VMS; do lxc exec "$vm" -- bash -c 'sync; echo 3 > /proc/sys/vm/drop_caches'; done
}

# timed_run <stage> <framework> <dataset> <output dir> <command> <rep>  -> prints secs
timed_run() {
  local stage=$1 fw=$2 ds=$3 out=$4 cmd=$5 rep=$6
  on_tiny "hdfs dfs -rm -r -f -skipTrash $out" > /dev/null 2>&1
  drop_caches
  on_tiny "/usr/bin/time -f %e -o /tmp/time $cmd" > $RES/${stage}_${fw}_${ds}_r$rep.log 2>&1
  local secs=$(on_tiny "tail -1 /tmp/time")
  echo "$stage,$fw,$ds,$rep,$secs" >> $CSV
  echo "$stage $fw $ds r$rep: ${secs}s" >&2
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

# MR and Spark must agree: same checksum after putting both outputs in canonical form
# agree <stage> <dataset> <canonical form> <MR dir> <Spark dir>
agree() {
  local a=$(on_tiny "hdfs dfs -cat $4/part-* | $3 | md5sum | cut -d' ' -f1")
  local b=$(on_tiny "hdfs dfs -cat $5/part-* | $3 | md5sum | cut -d' ' -f1")
  local ok=no; [ -n "$a" ] && [ "$a" = "$b" ] && ok=yes
  echo "$1,$2,$a,$b,$ok" >> $RES/agree.csv
  echo "$1 $2 MR vs Spark agree: $ok" >&2
}

for ds in ${DATASETS:-fw1 fw8 fw15}; do
  case $ds in
    fw1)  in="/fineweb/raw/000.parquet" ;;
    fw8)  in="/fineweb/raw/00[0-7].parquet" ;;
    fw15) in="/fineweb/raw/*.parquet" ;;
  esac
  T=/fineweb/tsv/$ds

  want B0      && run_stage B0 spark     $ds $T "spark-submit --class $P.B0Ingest $JAR '$in' $T"

  want B1mr    && run_stage B1 mapreduce $ds /pipe/b1mr/$ds    "hadoop jar $JAR $P.B1WordCountMR $T /pipe/b1mr/$ds $R"
  want B1spark && run_stage B1 spark     $ds /pipe/b1spark/$ds "spark-submit --class $P.B1WordCountSpark $JAR $T /pipe/b1spark/$ds $R"
  want B1spark && agree B1 $ds "LC_ALL=C sort" /pipe/b1mr/$ds /pipe/b1spark/$ds

  want B2mr    && run_stage B2 mapreduce $ds /pipe/b2mr/$ds    "hadoop jar $JAR $P.B2FilterDedupMR $T /pipe/b2mr/$ds $R"
  want B2spark && run_stage B2 spark     $ds /pipe/b2spark/$ds "spark-submit --class $P.B2FilterDedupSpark $JAR $T /pipe/b2spark/$ds $R"
  # output lines are input lines unchanged -> same set of ids = same output
  want B2spark && agree B2 $ds "cut -f1 | LC_ALL=C sort" /pipe/b2mr/$ds /pipe/b2spark/$ds
  # B3 reads the MR output, Spark output no longer needed once compared
  want B2spark && on_tiny "hdfs dfs -rm -r -f -skipTrash /pipe/b2spark/$ds" > /dev/null 2>&1

  want B3      && run_stage B3 spark     $ds /pipe/b3/$ds "spark-submit --class $P.$B3 $JAR /pipe/b2mr/$ds /pipe/b3/$ds $R"
  want B4      && run_stage B4 spark     $ds /pipe/b4/$ds "spark-submit --class $P.B4Report $JAR $T /pipe/b2mr/$ds /pipe/b3/$ds /pipe/b4/$ds"

  # data for 4d
  if want B4; then
    on_tiny "hdfs dfs -cat /pipe/b1mr/$ds/part-* | LC_ALL=C sort -t\$'\t' -k2,2nr | head -25" > $RES/top25_words_$ds.tsv
    for f in funnel top_domains tokens_per_dump; do
      on_tiny "hdfs dfs -cat /pipe/b4/$ds/$f/part-*" > $RES/${f}_$ds.csv
    done
  fi
done
