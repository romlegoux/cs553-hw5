#!/bin/bash
# push-hosts.sh — à lancer sur le node, une fois les VMs démarrées
set -euo pipefail

# Nom et IPv4 de chaque VM, ex. "small1,10.12.34.56 (enp5s0)"
lxc list --format csv -c n4 \
  | awk -F, '$2 != "" { split($2, a, " "); print a[1], $1 }' \
  > hosts.vms

# Fichier final : localhost + les VMs (pas de ligne 127.0.1.1)
{
  echo "127.0.0.1 localhost"
  cat hosts.vms
} > hosts.cluster

cat hosts.cluster   # pour vérifier

# Copie dans chaque VM
for vm in $(awk '{print $2}' hosts.vms); do
  lxc file push hosts.cluster "$vm/etc/hosts"
done
