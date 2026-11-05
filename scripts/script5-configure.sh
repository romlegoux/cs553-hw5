#!/bin/bash
set -euo pipefail
C=~/hw5/conf/cluster$1
H=/home/ubuntu/hadoop-3.5.0/etc/hadoop
S=/home/ubuntu/spark-4.2.0-bin-hadoop3/conf

for vm in $(lxc list --format csv -c n); do
  for f in core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml workers hadoop-env.sh; do
    lxc file push --uid 1000 --gid 1000 $C/$f $vm$H/$f
  done
  for f in spark-defaults.conf spark-env.sh; do
    lxc file push --uid 1000 --gid 1000 $C/$f $vm$S/$f
  done
  echo "config pushed -> $vm"
done

# new cluster -> new HDFS (the old NameNode metadata does not match the new DataNode)
lxc exec tiny -- sudo -H -u ubuntu bash -lc 'rm -rf ~/hdfs/namenode ~/hadoop-tmp && hdfs namenode -format -force -nonInteractive > /dev/null 2>&1 && echo "HDFS formatted"'

cd ~/hw5/scripts && ./start-cluster.sh

# archive of Spark jars used by spark.yarn.archive
lxc exec tiny -- sudo -H -u ubuntu bash -lc 'cd /tmp && jar cf0 spark-libs.jar -C $SPARK_HOME/jars/ . && hdfs dfs -mkdir -p /spark-jars && hdfs dfs -put -f spark-libs.jar /spark-jars/ && echo "spark jars uploaded"'
