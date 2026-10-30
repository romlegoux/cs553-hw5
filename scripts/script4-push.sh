#!/bin/bash
# script4-push.sh : à lancer depuis le node avec
#   lxc exec tiny -- sudo -iu ubuntu bash -s < script4-push.sh
set -e
HADOOP_HOME=/home/ubuntu/hadoop-3.5.0
CONF=$HADOOP_HOME/etc/hadoop
cd $CONF
for h in $(cat workers); do
  echo "push -> $h"
  scp -q core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml hadoop-env.sh workers $h:$CONF/
done
