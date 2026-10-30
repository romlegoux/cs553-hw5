#!/bin/bash
# setup-ssh.sh — à lancer sur le node
set -euo pipefail
U=ubuntu
VMS=$(lxc list --format csv -c n)

# 1. Paire de clés sur tiny (seulement si elle n'existe pas déjà)
lxc exec tiny -- sudo -u $U bash -c \
  '[ -f ~/.ssh/id_ed25519 ] || ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519 -q'

# 2. Récupérer la clé publique sur le node
lxc file pull tiny/home/$U/.ssh/id_ed25519.pub tiny.pub

# 3. L'ajouter dans authorized_keys de chaque VM (sans doublon, avec les bonnes permissions)
for vm in $VMS; do
  lxc exec "$vm" -- sudo -u $U bash -c '
    mkdir -p ~/.ssh && chmod 700 ~/.ssh
    touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys
    key=$(cat)
    grep -qxF "$key" ~/.ssh/authorized_keys || echo "$key" >> ~/.ssh/authorized_keys
  ' < tiny.pub
done

# 4. Sur tiny : accepter automatiquement les empreintes des VMs du cluster
lxc exec tiny -- sudo -u $U bash -c 'cat > ~/.ssh/config <<EOF
Host tiny small* large localhost
  StrictHostKeyChecking accept-new
EOF
chmod 600 ~/.ssh/config'
