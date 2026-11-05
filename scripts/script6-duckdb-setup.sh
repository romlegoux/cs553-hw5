#!/bin/bash
# script6-duckdb-setup.sh: DuckDB CLI + FineWeb Parquet on the large VM's local disk (run on the node)
set -euo pipefail
lxc exec large -- bash -c 'export DEBIAN_FRONTEND=noninteractive; apt-get install -yq unzip > /dev/null
  cd /tmp && wget -q https://github.com/duckdb/duckdb/releases/download/v1.4.1/duckdb_cli-linux-amd64.zip
  unzip -o -q duckdb_cli-linux-amd64.zip -d /usr/local/bin && duckdb --version'
lxc exec large -- sudo -H -u ubuntu bash -c '
  U=https://huggingface.co/datasets/HuggingFaceFW/fineweb/resolve/main/sample/10BT
  mkdir -p ~/fineweb ~/duck && cd ~/fineweb
  for i in $(seq -f %03g 0 14); do [ -s $i.parquet ] || wget -q -O $i.parquet $U/${i}_00000.parquet; echo "file $i"; done
  du -sh ~/fineweb'
