#!/bin/bash
set -uo pipefail
CLUSTER=B
VMS=$(lxc list --format csv -c n)
WORKERS=$(echo "$VMS" | grep -v '^tiny$')
RES=~/hw5/results/part2/cluster$CLUSTER; mkdir -p $RES
CSV=$RES/sorts.csv
[ -f $CSV ] || echo "cluster,sort,size_gb,rep,seconds,checksum_ok" > $CSV
EX=/home/ubuntu/hadoop-3.5.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.5.0.jar
JAR=/home/ubuntu/hw5-1.0.jar
R=55   # 8 workers x 7 containers - 1 AM

# run a command on tiny as ubuntu, with Hadoop env
on_tiny() { lxc exec tiny -- sudo -H -u ubuntu bash -lc "$1"; }

drop_caches() {
  sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
  for vm in $VMS; do lxc exec "$vm" -- bash -c 'sync; echo 3 > /proc/sys/vm/drop_caches'; done
}

# sar every second on each worker (4b)
start_sar() {
  date +%s > $RES/$1.start
  for vm in $WORKERS; do
    lxc exec $vm -- bash -c "setsid -f sar -u -r -d -n DEV -o /tmp/$1.sar 1 < /dev/null > /dev/null 2>&1"
  done
}
stop_sar() {
  for vm in $WORKERS; do
    lxc exec $vm -- pkill -x sar
    lxc file pull $vm/tmp/$1.sar $RES/$1_$vm.sar
  done
}

# run_sort <size> <sort> <rep> <ref checksum>  -> prints secs on stdout
run_sort() {
  local size=$1 sort=$2 rep=$3 ref=$4
  local in=/tera/in$size out=/tera/$sort$size val=/tera/val$size
  local tag=${sort}_${size}GB_r$rep
  local name=$RES/$tag
  local monitor=no; [ $size = 64 ] && [ $sort != terasort ] && monitor=yes

  case $sort in
    terasort)  cmd="hadoop jar $EX terasort -Dmapreduce.job.reduces=$R $in $out" ;;
    mrsort)    cmd="hadoop jar $JAR edu.iit.cs553.hw5.MRSort $in $out $R" ;;
    sparksort) cmd="spark-submit --class edu.iit.cs553.hw5.SparkSort --jars $EX $JAR $in $out $R" ;;
  esac

  # clean + cold start
  on_tiny "hdfs dfs -rm -r -f -skipTrash $out ${out}_partitions $val" > /dev/null 2>&1
  drop_caches

  # timed run
  [ $monitor = yes ] && start_sar $tag
  on_tiny "/usr/bin/time -f %e -o /tmp/time $cmd" > $name.log 2>&1
  [ $monitor = yes ] && stop_sar $tag
  local secs=$(on_tiny "tail -1 /tmp/time")

  # validate (not timed)
  on_tiny "hadoop jar $EX teravalidate $out $val > /dev/null 2>&1; hdfs dfs -cat $val/part-r-00000" > $name.validate
  local cks=$(grep checksum $name.validate | cut -f2)
  local ok=no
  [ "$cks" = "$ref" ] && ! grep -q '^error' $name.validate && ok=yes

  echo "$CLUSTER,$sort,$size,$rep,$secs,$ok" >> $CSV
  echo "$sort ${size}GB r$rep: ${secs}s checksum_ok=$ok" >&2

  # free disk
  on_tiny "hdfs dfs -rm -r -f -skipTrash $out ${out}_partitions $val" > /dev/null 2>&1

  echo $secs
}

# ---- experiments ----
for size in 16 32 64; do
  recs=$((size * 10000000))   # 16 GB = 160 000 000 records of 100 B

  # generate input (not timed)
  on_tiny "hdfs dfs -rm -r -f -skipTrash /tera/in$size; hadoop jar $EX teragen -Dmapreduce.job.maps=64 $recs /tera/in$size" > $RES/teragen_$size.log 2>&1

  # input checksum (only the checksum line: input is unsorted, full output is huge)
  ref=$(on_tiny "hadoop jar $EX teravalidate /tera/in$size /tera/valin$size > /dev/null 2>&1; hdfs dfs -cat /tera/valin$size/part-r-00000 | grep checksum | cut -f2")
  echo "$size,$ref" >> $RES/input_checksums.csv
  on_tiny "hdfs dfs -rm -r -f -skipTrash /tera/valin$size" > /dev/null 2>&1
  echo "input ${size}GB checksum: $ref" >&2

  for sort in terasort mrsort sparksort; do
    secs=$(run_sort $size $sort 1 "$ref")
    s=${secs%.*}
    # < 10 min -> 2 more runs
    if [ -n "$s" ] && [ "$s" -lt 600 ]; then
      run_sort $size $sort 2 "$ref" > /dev/null
      run_sort $size $sort 3 "$ref" > /dev/null
    fi
  done

  # free disk before next size
  on_tiny "hdfs dfs -rm -r -f -skipTrash /tera/in$size" > /dev/null 2>&1
done
