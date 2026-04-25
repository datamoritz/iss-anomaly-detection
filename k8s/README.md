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

- Postgres manifests
- Redis manifests
- Kafka manifests
- Ingress / TLS
- Persistent volumes for stateful services

The manifests are intended for K3s as well as standard Kubernetes.

## Assumptions

- A backend image is built and pushed to a registry, then the image field is updated from:
  - `iss-datacenters-backend:latest`
- `DATABASE_URL` is the preferred Postgres connection setting.
- `PROTOTYPE_LIBRARY_DIR` points to the read-only bundled image path:
  - `/app/prototype-library/smap_final9_v01`
- Writable runtime paths can stay ephemeral for the stateless services in this first pass.

## Apply

```bash
kubectl apply -k k8s/base
```

## Next steps

1. Replace placeholder image and secret values.
2. Add stateful services or point these Deployments at external Postgres/Redis/Kafka.
3. Add Ingress for the API and WebSocket endpoint.
4. Add resource tuning and, later, service-specific probes for non-HTTP workers if needed.
