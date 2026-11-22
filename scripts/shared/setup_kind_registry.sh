#!/usr/bin/env bash
# Configure the registry for an EXISTING cluster without creating/deleting nodes. Nodes missing config_path: enable it once and restart
# containerd in the node (running containers survive thanks to the shim, kubelet reconnects itself).
# Theo https://kind.sigs.k8s.io/docs/user/local-registry/ (containerd hosts.toml).
set -euo pipefail
cd "$(dirname "$0")/../.."
cluster=${KIND_CLUSTER:-platform}
nodes=$(kind get nodes --name "$cluster")
[[ -n "$nodes" ]] || { echo "Cluster $cluster does not exist yet; leaving infrastructure untouched." >&2; exit 1; }
for node in $nodes; do
  if ! docker exec "$node" containerd config dump | grep -E "config_path = [\"']/etc/containerd/certs.d" >/dev/null; then
    echo "$node: enabling config_path=/etc/containerd/certs.d, then restarting containerd"
    docker exec -i "$node" sh -c 'cat >> /etc/containerd/config.toml' <<'TOML'

[plugins."io.containerd.grpc.v1.cri".registry]
  config_path = "/etc/containerd/certs.d"
TOML
    docker exec "$node" systemctl restart containerd
    until docker exec "$node" crictl info >/dev/null 2>&1; do sleep 2; done
  fi
done
docker compose --profile ml up -d --wait registry
for node in $nodes; do
  docker exec "$node" mkdir -p /etc/containerd/certs.d/localhost:5001
  docker exec -i "$node" cp /dev/stdin /etc/containerd/certs.d/localhost:5001/hosts.toml <<'HOSTS'
server = "http://registry:5000"
[host."http://registry:5000"]
  capabilities = ["pull", "resolve"]
HOSTS
done
kubectl --context "kind-$cluster" apply -f - <<'YAML'
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:5001"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
YAML
printf 'Registry localhost:5001 → registry:5000, configured %s\n' "$cluster"
