# craft storage k8s test harness (STAGED — do NOT apply until preconditions hold)

## 1. DAV endpoint (probed 2026-10-14 from ephemeral pods in ns `debug`)

- In-cluster endpoint: `http://proxy.ocis.svc.cluster.local:9200` (oCIS `proxy` Service, plain HTTP)
- Paths (both routed + auth-gated):
  - `/dav/spaces` — modern Spaces API (PROPFIND Depth 0/1)
  - `/remote.php/dav/files/<username>/<path>` — legacy per-user path (used by the round-trip Job; no drive-id discovery needed)
- Routing signal: `PROPFIND /dav/spaces` without auth -> **401** with `Www-Authenticate: Bearer`;
  `PROPFIND` on a bogus path -> **200** (falls through to the web fallback), so 401s are the DAV gate, not blanket behavior.
- External equivalent: `https://files.harryslab2.xyz` (same proxy via Traefik); harness uses service DNS (no TLS, no hairpin).

## 2. Verified status codes (test user `ocis-e2e-tmp`, creds out-of-band only)

| Probe | Result |
|---|---|
| `PROPFIND /dav/spaces`, no auth | **401** (+ `Www-Authenticate: Bearer`) |
| `PROPFIND /dav/spaces`, Basic with LDAP-valid password | **401** |
| `PROPFIND /dav/spaces`, Basic with stale passwords (`admin`, `akadmin`) | **401** |
| `PROPFIND /dav/spaces`, garbage Bearer | **401** |
| `PROPFIND /dav/spaces`, valid OIDC Bearer (userinfo **200** as `ocis-e2e-tmp`) | **401** |
| LDAP simple-bind as the test user (same password) | **success** (bind ok; `whoami` exop unimplemented by outpost — irrelevant) |
| PUT/GET/PROPFIND/DELETE file round-trip | **NOT YET RUN — blocked (see 3)** |

## 3. Auth findings for the StorageBackend sibling

- **Basic auth does not work in this deployment.** The password is fine at the LDAP layer,
  but oCIS rejects it at DAV. The `authservice` deployment carries machine-auth config only
  (no user-password backend); with external user management the proxy cannot validate passwords.
  Design the StorageBackend for **Bearer (OIDC) tokens**, not username/password.
- **Bearer also 401s today because the test account is not provisioned in oCIS.**
  `values/ocis-values.yaml` has `features.externalUserManagement.autoprovisionAccounts.enabled: false`,
  so `ocis-e2e-tmp` (active in Authentik, visible in LDAP as `cn=ocis-e2e-tmp,ou=users,dc=harryslab2,dc=xyz`,
  username schema `cn`) is unknown to oCIS. Remediation (Harry/ops call, NOT part of this harness):
  provision the account (or temporarily enable autoprovisioning), then re-run the probe:
  `PROPFIND /dav/spaces` with a fresh Bearer token must return **207** before the Jobs below can pass.
- Token lifetime note: Authentik access tokens for provider `oCIS` (public client id `ocis`,
  grants `authorization_code`+`refresh_token`, NO device-code grant) live **1h** — mint the
  `craft-storage-test-dav` secret fresh at launch; do not reuse stale tokens.
- Incidental: `authentik-server` restarted mid-probe (7 restarts); transient LDAP 49s during a
  restart are server flaps, not credential failures. `ocis-e2e-tmp` password was reset during
  probing; the new value is stashed out-of-band (see 4), file name only.

## 4. Out-of-band inputs (never in git)

- `~/.secrets/talos-cluster/craft/e2e-dav-password` — test-user password (file name only; chmod 600).
- `craft-storage-test-dav` Secret in ns `craft-storage-test`, key `token` — created at launch (see 5).
- `forgejo-registry` imagePullSecret — copied from ns `craft` at launch (see 5).

## 5. Launch (exact commands; run when the new binary image is ready AND §3 blocker cleared)

```bash
export KUBECONFIG=/home/ubuntu/.secrets/talos-cluster/kubeconfig
K=/opt/data/bin/linux-amd64/kubectl
D=/home/ubuntu/.hermes/cache/scratch/craft/dev/k8s-test

# 0. Preconditions: wordcraft-cli-test image tag updated in job-wordcraft-cli.yaml,
#    CLI subcommand confirmed, test user provisioned in oCIS, fresh OIDC token at hand.

# 1. Create namespace + copy registry pull secret (values never printed):
$K apply --dry-run=server -f $D   # re-validate after any edit
$K create namespace craft-storage-test
$K -n craft get secret forgejo-registry -o yaml \
  | sed 's/namespace: craft/namespace: craft-storage-test/' \
  | $K apply -f -

# 2. Create the DAV token secret from a FRESH token ($TOKEN in env, never echoed):
$K -n craft-storage-test create secret generic craft-storage-test-dav \
  --from-literal=token="$TOKEN"

# 3. Launch:
$K apply -f $D
$K -n craft-storage-test wait --for=condition=complete job/dav-roundtrip --timeout=180s
$K -n craft-storage-test logs job/dav-roundtrip
$K -n craft-storage-test wait --for=condition=complete job/wordcraft-cli-storage-test --timeout=300s
$K -n craft-storage-test logs job/wordcraft-cli-storage-test

# 4. Staging web (no Ingress on purpose); check via port-forward:
$K -n craft-storage-test port-forward svc/craft-web-test 8080:80
curl -sS localhost:8080/healthz

# 5. Teardown:
$K delete namespace craft-storage-test
```

Files here: `namespace.yaml`, `job-dav-roundtrip.yaml`, `job-wordcraft-cli.yaml`,
`craft-web-test.yaml` (Deployment `craft-web-test` + Service, no Ingress), this README.
