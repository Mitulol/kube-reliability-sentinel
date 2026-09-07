#!/usr/bin/env bash
# End-to-end check: apply the chaos workloads, then assert that Sentinel
# detected each failure mode via its Prometheus metrics and emitted the
# corresponding Kubernetes Events. Used both by CI and by hand during
# local validation.
set -euo pipefail

NS_SENTINEL=sentinel
TIMEOUT="${TIMEOUT:-180}"

echo "==> applying chaos workloads"
kubectl apply -f "$(dirname "$0")/chaos-workloads.yaml"

# The image is distroless (no shell, no curl) and the namespace enforces
# PodSecurity "restricted" (so a throwaway busybox pod is rejected), so
# scrape /metrics over a short-lived port-forward from the host running
# this script.
metrics() {
  local pf
  kubectl -n "$NS_SENTINEL" port-forward svc/kube-reliability-sentinel 18080:8080 >/dev/null 2>&1 &
  pf=$!
  sleep 2
  curl -s --max-time 5 http://127.0.0.1:18080/metrics || true
  kill "$pf" 2>/dev/null || true
  wait "$pf" 2>/dev/null || true
}

# Wait until sentinel_alerts_fired_total for a given reason is >= 1.
wait_for_reason() {
  local reason="$1" deadline=$((SECONDS + TIMEOUT))
  echo "==> waiting for alert reason=$reason (<= ${TIMEOUT}s)"
  while (( SECONDS < deadline )); do
    if metrics | grep -E "sentinel_alerts_fired_total\{reason=\"$reason\"" | \
       awk '{ exit !($NF+0 >= 1) }'; then
      local t=$((SECONDS))
      echo "    detected reason=$reason"
      return 0
    fi
    sleep 3
  done
  echo "!! timed out waiting for reason=$reason"
  metrics | grep sentinel_alerts_fired_total || true
  return 1
}

fail=0
for r in CrashLoopBackOff OOMKilled PodStuckPending ImagePullError; do
  wait_for_reason "$r" || fail=1
done

echo "==> Kubernetes Events emitted by Sentinel:"
kubectl get events -A --field-selector reportingComponent=kube-reliability-sentinel \
  -o custom-columns=NS:.involvedObject.namespace,OBJ:.involvedObject.name,REASON:.reason,MSG:.message \
  2>/dev/null || kubectl get events -n chaos

echo "==> /metrics alert counters:"
metrics | grep -E 'sentinel_alerts_(fired|suppressed)_total|sentinel_unhealthy_pods' | grep -v '^#'

exit $fail
