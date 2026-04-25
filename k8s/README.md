# Kubernetes / K3s Base

This directory contains the first Kubernetes-ready base for the stateless app services:

- `iss-api`
- `iss-worker`
- `iss-ingest`
- `iss-notifications`
- `iss-injections`

Scope of this first pass:

- namespace
- shared config
- shared secrets placeholder
- Deployments for stateless services
- ClusterIP Service for the API

Not included yet:
- Ingress / TLS
- Persistent volumes for stateful services

The manifests are intended for K3s as well as standard Kubernetes.

It now also includes a simple single-node stateful layer for:

- `postgres`
- `redis`
- `kafka`

This stateful layer is intentionally minimal and designed for a single-node K3s setup,
not for production-grade multi-node Kafka or HA database deployment.

## Assumptions

- A backend image is built and pushed to a registry, then the image field is updated from:
  - `iss-datacenters-backend:latest`
- `DATABASE_URL` is the preferred Postgres connection setting.
- `PROTOTYPE_LIBRARY_DIR` points to the read-only bundled image path:
  - `/app/prototype-library/smap_final9_v01`
- Writable runtime paths can stay ephemeral for the stateless services in this first pass.

## Apply

Stateless app layer only:

```bash
kubectl apply -k k8s/base
```

Stateful + app together:

```bash
kubectl apply -k k8s/all
```

## Next steps

1. Replace placeholder image and secret values.
2. Add Ingress for the API and WebSocket endpoint.
3. Add storage classes / PVC sizing tuned to the target Hetzner server.
4. Add resource tuning and, later, service-specific probes for non-HTTP workers if needed.
