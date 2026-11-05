#!/bin/bash
# install-software.sh — à lancer sur le node
set -euo pipefail

U=ubuntu
HADOOP_VER=3.5.0
SPARK_VER=4.2.0
HADOOP_TGZ=hadoop-$HADOOP_VER.tar.gz
SPARK_TGZ=spark-$SPARK_VER-bin-hadoop3.tgz
JAVA_HOME_VM=/usr/lib/jvm/java-17-openjdk-amd64
VMS=${VMS:-$(lxc list --format csv -c n)}

# 1. Télécharger une seule fois sur le node (commandes de l'énoncé)
mkdir -p ~/dl ~/logs/install && cd ~/dl
[ -f $HADOOP_TGZ ] || wget https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VER/$HADOOP_TGZ
[ -f $SPARK_TGZ ]  || wget https://archive.apache.org/dist/spark/spark-$SPARK_VER/$SPARK_TGZ

# 2. Variables d'environnement, lues par tous les shells de login
cat > hadoop-profile.sh <<EOF
export JAVA_HOME=$JAVA_HOME_VM
export HADOOP_HOME=/home/$U/hadoop-$HADOOP_VER
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export SPARK_HOME=/home/$U/spark-$SPARK_VER-bin-hadoop3
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$SPARK_HOME/bin
EOF

# 3. Installation dans une VM
install_vm() {
  local vm=$1
  # Paquets de l'énoncé
  lxc exec "$vm" -- bash -c 'export DEBIAN_FRONTEND=noninteractive
    apt-get update -q &&
    apt-get install -yq openjdk-17-jdk-headless maven sysstat time python3-pip'

  # Archives et profil (fichiers poussés appartenant à ubuntu, uid 1000)
  lxc file push --uid 1000 --gid 1000 $HADOOP_TGZ $SPARK_TGZ "$vm/home/$U/"
  lxc file push hadoop-profile.sh "$vm/etc/profile.d/hadoop.sh"

  # Décompression dans ~ubuntu, en tant qu'ubuntu
  lxc exec "$vm" -- sudo -H -u $U bash -c "cd ~ &&
    tar xzf $HADOOP_TGZ && tar xzf $SPARK_TGZ && rm $HADOOP_TGZ $SPARK_TGZ &&
    echo 'export JAVA_HOME=$JAVA_HOME_VM' >> hadoop-$HADOOP_VER/etc/hadoop/hadoop-env.sh"
}

# 4. Lancer les 9 installations en parallèle, un log par VM
declare -A PIDS
for vm in $VMS; do
  install_vm "$vm" > ~/logs/install/$vm.log 2>&1 &
  PIDS[$vm]=$!
done

FAIL=0
for vm in "${!PIDS[@]}"; do
  if wait "${PIDS[$vm]}"; then echo "OK    $vm"
  else echo "ECHEC $vm (voir ~/logs/install/$vm.log)"; FAIL=1; fi
done
exit $FAIL
