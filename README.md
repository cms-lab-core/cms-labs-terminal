# cms-labs-terminal

Namespace-local browser terminals for CMS Labs. One broker process owns one Kubernetes PTY per
configured topology node, so closing or reloading a browser does not kill the shell, its current
directory, exported variables, foreground/background processes, or recent output.

Nothing is copied into a network device and nothing is written to its filesystem.

## Architecture

```text
CMS UI
  │ workspace grant + scoped HttpOnly cookie
  ▼
Clabgate / nginx auth_request
  │ verified X-CMS-Identity
  ▼
node Service :7681 ──► ttyd listener ──► short-lived `connect` helper
                                             │ loopback HTTP full duplex
                                             ▼
                                      long-lived Go broker
                                             │ one stream per target
                                             ▼
                                  Kubernetes pods/exec or pods/attach
```

- `ttyd` remains the maintained WebSocket/xterm.js frontend. It may start a child for every browser;
  that child owns no shell and only bridges stdio to the broker.
- The bridge puts ttyd's local pseudo-terminal into raw mode before forwarding bytes, so line
  buffering and local echo cannot interfere with the remote CLI.
- The broker owns the remote stream. A browser detach closes only its `Client`; the Kubernetes stream
  remains alive until the configured lease expires, the remote process exits, or the broker shuts down.
- A second browser takes over the existing PTY. The previous browser is detached, so two tabs cannot
  interleave input into one network CLI.
- Output is always appended to a bounded raw scrollback. Output produced while no browser is attached
  is replayed on reconnect. A bounded number of completed-session tails is retained as history.
- The internal bridge binds a loopback address and is never exposed by a Service.

The broker intentionally does not persist across a terminal Pod restart. Making that guarantee would
require moving a live PTY stream to another process, which Kubernetes exec cannot do. The Deployment is
therefore one replica with a `Recreate` strategy; browser reloads and network breaks survive, broker or
device Pod replacement does not.

## Why this small broker remains custom

`ttyd`, WeTTY and Kubebox provide a browser terminal but create a new remote terminal when a connection
is opened. Teleport provides Kubernetes access, identity, audit and recording as a complete access
platform; adopting it would replace rather than integrate with the existing Clabgate grant/cookie and
per-attempt namespace model. A server-side multiplexer would add another process and filesystem state
without removing the need to resolve targets and hold Kubernetes streams.

The custom surface here is consequently narrow: registry/lease/scrollback, Kubernetes stream lifetime,
and a loopback transport. Browser terminal emulation remains in ttyd and Kubernetes protocol handling
remains in `client-go`.

## Configuration

```yaml
server:
  address: 0.0.0.0
  title: Network lab
  authHeader: X-CMS-Identity
  maxClients: 4

broker:
  address: 127.0.0.1:17680
  lease: 30m
  reapInterval: 1m
  scrollbackBytes: 1048576
  history: 3

access:
  namespaces: [lab-00000000]
  nodeLabel: c9s.run/topologyNode
  ownerLabel: c9s.run/topologyOwner
  owner: lab-00000000

session:
  mode: exec
  command: [/bin/sh]

targets:
  - name: srl1
    label: SR Linux 1
    port: 7681
    mode: exec
    command: [sr_cli]
  - name: linux1
    label: Linux host
    port: 7682
    mode: exec
    command: [/bin/bash, -l]
  - name: alpine1
    label: Alpine host
    port: 7683
    mode: exec
    command: [/bin/ash, -l]
```

Every target must be declared. A target may pin `namespace`, `pod` and `container`; otherwise its pod is
resolved from the configured node/owner labels and its container from
`kubectl.kubernetes.io/default-container`. Wildcard namespaces are rejected because this service is
deployed once per attempt namespace.

`mode: exec` starts exactly `command` through the Kubernetes exec API. No implicit shell is inserted.
`mode: attach` has no command and joins the container's existing process; that container must have stdin
and TTY enabled in its Pod spec.

Each target has its own internal listener port. For compatibility with the current CMS frontend, create
one ClusterIP Service per target with external port name `ttyd`, port `7681`, and a `targetPort` pointing
at that target's listener. All Services select the same terminal Pod. See
[`deploy/manifests.yaml`](deploy/manifests.yaml).

The chart is published as OCI together with tagged releases:

```sh
helm upgrade --install terminal \
  oci://ghcr.io/cms-lab-core/charts/cms-labs-terminal \
  --namespace lab-00000000 \
  --set-json 'targets=[{"name":"srl1","port":7681,"mode":"exec","command":["sr_cli"]}]'
```

In production, also enable `networkPolicy` and set its namespace/pod selectors to the trusted CMS
frontend proxy. Clabgate can render the same target list while it creates the attempt namespace.

## Authentication with current Clabgate

The terminal is not another OIDC client. CMS/Clabgate already knows the user and owns authorization:

1. `session.open`/`topology.get` checks ownership and issues a short-lived workspace grant.
2. Clabgate exchanges it for a session-scoped HttpOnly cookie.
3. nginx runs `workspace-auth/verify` for ttyd HTTP and WebSocket requests.
4. nginx overwrites `X-CMS-Identity` with the verified response header.
5. `ttyd --auth-header X-CMS-Identity` rejects a direct request without that trusted header and exports
   the value to the helper as `TTYD_USER` for attach/detach audit metadata.

Do not expose a target Service through an Ingress directly. The auth header is meaningful only when the
terminal Service is reachable exclusively through the trusted proxy; production clusters should also
apply a NetworkPolicy allowing ingress only from that proxy.

`server.allowUnauthenticated: true` exists only for a developer using `kubectl port-forward` and must not
be used in shared environments.

## Commands

```sh
# Server process used by the Deployment.
cms-labs-terminal serve --config /etc/cms-labs-terminal/config.yaml

# Resolve a declared target without opening a terminal.
cms-labs-terminal resolve --config ./config.yaml srl1

# Print validated listeners and broker limits.
CMS_LABS_TERMINAL_CONFIG=./config.yaml cms-labs-terminal diagnose
```

`connect` is an internal mode launched by ttyd. It is deliberately not a public Kubernetes client and
can only reach the broker's loopback endpoint.

## Verification

```sh
gofmt -l .
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...

docker build -t cms-labs-terminal:dev .
./scripts/verify-image.sh cms-labs-terminal:dev
```

Broker tests use in-memory fake processes and cover reload with one start, preserved input state,
single-client takeover, detached output replay, lease behavior, bounded history, per-target isolation,
resize, concurrent first attach, failed-start retry, and shutdown. A live-cluster smoke test is still
needed for the Kubernetes API server's exec/attach transport itself.
