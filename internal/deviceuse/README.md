# CompositeDisk existing-source use gates

`compositedisk.provider.v1.DeviceUse` exposes only AcquireUse, GetUse and
ReleaseUse. Requests contain a persisted opaque printable ASCII use ID
(1-128 bytes), full composite CSI handle including CR UID, node ID, and exact
CSI driver/volume handle/PV name/PV UID. Responses contain only that identity
and ACTIVE or RELEASED. This is a teardown gate, **not storage readiness**.

No custom capacity, reservation, attach, path lookup, renewal, epoch, custody,
SCM_RIGHTS, or cleanup-receipt service remains. Standard Block CSI staging and
publication provide the private node path; CompositeDisk validates and owns
its actual FDs and mapper lifecycle.

## Durable ownership

Fresh AcquireUse reads authentic Bound Block/RWO PV/PVC bindings, configured
namespace/node UIDs and placement, and actual ordinary VG/LV UUIDs. Another
live use of that actual resource is rejected, even through a different PV.
ACTIVE replay revalidates identity. Missing metadata fails acquisition but
never deletes a hold.

GetUse reports exact durable state; NotFound is not cleanup evidence.
Authenticated exact-owner ReleaseUse follows trusted CompositeDisk physical
cleanup and FD closure. Existing retained ownership can be explicitly released
after source metadata disappears; absence itself never triggers release.
An early release of a never-seen ID requires live scope validation and writes
a permanent RELEASED tombstone. Late Acquire cannot rearm it, and replaying its
release cannot clear a different active use. New activation requires a new ID.
No clock, process exit, node NotReady state or disconnect clears ownership.

The file journal serializes use changes and full native operations with a
directory-inode flock, fsynced snapshot, atomic rename, and directory fsync.
Native CSI unpublish/unstage/delete/expand, LVM destruction/quarantine/wipe,
and failover/orphan GC retain durable admission until confirmed completion.
Nested helpers carry the same private context token across actual effects;
they are not check-then-act gates. Unknown native outcomes survive restart and
block new acquisition. Successful completion is recorded before returning the
CSI reply; late completion cannot clear a successor operation.

Existing-source Block NodeStage/NodePublish still work while the use is ACTIVE
on its owning node, with no private token in CSI metadata. Ordinary
NodeEnsureVolume behavior remains: standard CreateVolume calls EnsureVolume
before returning, so successful ordinary provisioning materializes the LV
before PV binding. Acquisition accepts an existing inactive LV that needs
normal staging/activation. **Bound metadata alone is insufficient:** a stale or
failed-over PV whose LV is missing cannot be admitted and then recreated under
a hold. Source provisioning/recovery must happen independently first.

Release the use after real composite cleanup but **before** native child
unpublish/unstage. New acquisition racing that teardown either wins and blocks
the old teardown, or loses to durable native admission and must retry. Unknown
native effects have no automatic recovery or forced node-loss unlock.

## Opt-in node configuration

`--device-use-config=/etc/acs-compositedisk-use/domain.json` enables the service
and gates together. Without it, normal upstream behavior and official
`localdisk.csi.acstor.io` identity are unchanged. Existing separate canary
registration and exclusive native VG isolation remain available; a different
driver name alone is not physical isolation from a managed addon.

```json
{
  "scope": {
    "Driver": "localdisk.compositedisk.infinitydb.io",
    "Namespace": "applications", "NamespaceUID": "<namespace UID>",
    "Node": "worker-a", "NodeUID": "<node UID>"
  },
  "journalDirectory": "/var/lib/acs-compositedisk-use",
  "readerKubeconfig": "/etc/acs-compositedisk-use/reader.kubeconfig",
  "endpoint": ":9711",
  "serverCertFile": "/etc/acs-compositedisk-use/tls.crt",
  "serverKeyFile": "/etc/acs-compositedisk-use/tls.key",
  "clientCAFile": "/etc/acs-compositedisk-use/client-ca.crt",
  "grants": [{
    "uri": "spiffe://infinity/compositedisk/node/worker-a",
    "node": "worker-a", "namespace": "applications",
    "pvNames": ["existing-cache-pv"]
  }]
}
```

Configuration/credential files must be root-owned and not group/world writable.
The independent reader requires exactly one explicit identity/context, HTTPS
with embedded CA, and dedicated token or embedded mTLS; no ambient fallback,
exec plugins, insecure TLS or proxies. Read permissions are get
nodes/namespaces/PVs and same-namespace PVCs, not source writes/deletes.

Provision an empty persistent root-owned 0700 directory, then call
`deviceuse.Initialize(directory, scope)` once offline. It creates canonical
version-1 `use-ledger.json` with mode 0600. Startup never initializes missing
state. Missing/incompatible/duplicate-field journals fail closed, unchanged.
Do not reset, restore or move the journal independently of its media, or run
another journal/unguarded driver over the same physical source. Keep all native
actors in the same authority domain. There is no fence against privileged
out-of-band storage mutation.
Initialization also refuses a nonempty directory so a new journal cannot
silently shadow unresolved old state. Native CSI has no use token; exclude
competing source consumers independently.
The prototype retains at most 4096 use records and 768 KiB of history, reserving
extra space for native completion/cleanup. Exhaustion fails new uses closed;
it never prunes tombstones or releases an existing use.

TLS 1.3 mutual authentication requires an exact independently configured
certificate URI, node, namespace and PV-name grant. `readOnly:true` permits
GetUse only. Certificate expiry does not release ownership. The Helm
`deviceUse.enabled` option defaults false and mounts protected configuration
and persistent state when enabled; its port must match `endpoint`. For a
separate canary, retain distinct CSI registration/plugin/socket paths, the
preprovisioned private VG and its driver-specific tag; qualify addon exclusion
independently. No cluster changes are performed by these files.

The explicit PV-name grants require an operator configuration refresh for new
template-generated PV names. There is no implicit namespace wildcard.
A separately registered canary retains its ordinary isolation bootstrap:

```text
--driver-name=localdisk.compositedisk.infinitydb.io
--canary-volume-group=compositedisk-canary
--device-use-config=/etc/acs-compositedisk-use/domain.json
--csi-node-bind-address=unix:///csi-canary/node.sock
--csi-controller-bind-address=unix:///csi-canary/controller.sock
```

Its source StorageClass uses the same driver and explicit `volumeGroup`, with
the preprovisioned VG tagged `compositedisk.csi.localdisk.compositedisk.infinitydb.io`
and not `local-csi`. Keep registrar paths, CSIDriver, RBAC and host plugin mounts
separate from the managed addon; the default chart does not perform that
isolation conversion automatically.

The old DeviceProvider protocol/store is not migrated or aliased. Do not mix
versions or start an empty journal over unresolved old holds. The noncompiled
historical files under `../devicelease/archive/owned-backing-pvcs/` remain
unchanged. The retired `acs-compositedisk-raw-v1` VG remains excluded from
ordinary CSI and GC; removing its old allocator must not adopt unknown media.
Regenerate `usev1/device_use.proto` with
`sh internal/deviceuse/usev1/generate.sh` using protoc 3.12.4,
protoc-gen-go 1.36.12 and protoc-gen-go-grpc 1.5.1.

Run focused, unprivileged Linux tests with native filesystem TMPDIR/GOTMPDIR:

```sh
go test -buildvcs=false -race -p=2 -count=1 -timeout=180s ./internal/deviceuse/... ./internal/csi/node/... ./internal/csi/controller/... ./internal/csi/core/lvm/... ./internal/gc/... ./cmd/driver/...
go build -buildvcs=false -p=2 ./cmd/driver ./cmd/manager
```

Tests use regular files, fake Kubernetes state and mocked native operations;
they do not grant or qualify physical storage readiness.
