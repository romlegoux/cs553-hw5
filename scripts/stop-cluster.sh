#!/bin/bash
lxc exec tiny -- sudo -iu ubuntu bash -s <<'EOF'
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/home/ubuntu/hadoop-3.5.0
export SPARK_HOME=/home/ubuntu/spark-4.2.0-bin-hadoop3
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$PATH

echo ">> Spark"; $SPARK_HOME/sbin/stop-history-server.sh
echo ">> MapReduce "; mapred --daemon stop historyserver
echo ">> YARN"; stop-yarn.sh
echo ">> HDFS"; stop-dfs.sh

echo; echo "== proc remaining on tiny =="; jps | grep -v Jps || echo "(ok)"
EOF
