#!/bin/bash
lxc exec tiny -- sudo -iu ubuntu bash -s <<'EOF'
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/home/ubuntu/hadoop-3.5.0
export SPARK_HOME=/home/ubuntu/spark-4.2.0-bin-hadoop3
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$PATH

start-dfs.sh
hdfs dfsadmin -safemode wait           
start-yarn.sh
mapred --daemon start historyserver
hdfs dfs -mkdir -p /spark-logs
$SPARK_HOME/sbin/start-history-server.sh

jps | grep -v Jps
for h in $(cat $HADOOP_HOME/etc/hadoop/workers); do
  echo "== $h =="; ssh -n $h jps | grep -v Jps
done
echo; hdfs dfsadmin -report | grep -i "live datanodes"
yarn node -list 2>/dev/null | head -1
EOF
