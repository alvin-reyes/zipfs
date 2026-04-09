#!/bin/sh
set -e

# Write cluster config if CLUSTER_PEERS is set
if [ -n "$CLUSTER_PEERS" ]; then
  cat > "$IPFS_PATH/config.json" <<CONF
{
  "gateway_port": ${GATEWAY_PORT:-${PORT:-8080}},
  "listen_addrs": ["/ip4/0.0.0.0/tcp/${SWARM_PORT:-4001}"],
  "announce_addrs": [],
  "cluster_peers": [${CLUSTER_PEERS}],
  "cluster_secret": "${CLUSTER_SECRET}",
  "cluster_mode": "${CLUSTER_MODE:-replicate}",
  "replication_factor": ${REPLICATION_FACTOR:-2},
  "self_heal_interval_secs": ${SELF_HEAL_INTERVAL:-60}
}
CONF
  echo "Cluster config written to $IPFS_PATH/config.json"
  cat "$IPFS_PATH/config.json"
fi

exec zipfs daemon
