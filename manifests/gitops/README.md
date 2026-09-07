# GitOps deployment (illustrative)

These two files show how Sentinel is meant to be delivered in a cluster that
is already running a GitOps controller: point Flux or Argo CD at
`manifests/` in this repo and let it reconcile.

**Status: illustrative, not operated.** They are written to the current
Flux / Argo CD API shapes and `kubectl apply --dry-run=server` clean, but
this project's real-cluster validation (see the main README) deployed
Sentinel with `kubectl apply -k manifests/` directly. No live Flux or
Argo CD pipeline was run against it. Treat these as "here is the wiring,"
not "here is a pipeline I operated."

- `flux-kustomization.yaml` — a Flux `GitRepository` + `Kustomization`.
- `argocd-application.yaml` — an Argo CD `Application`.
