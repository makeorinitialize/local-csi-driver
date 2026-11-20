# Existing-source PVC holds and native custody

The active canary resolves **externally owned, already Bound origin and cache
PVCs**. It owns use authorization, retained descriptors and cleanup evidence,
not the PVCs, PVs or physical provisioning lifecycle. Composite deletion must
close only its mappings/holds; it must never delete either source PVC/PV.

V1 requires a preformatted ext4 origin and an existing raw Block cache, both
compatible with the independently pinned node. ACS does not format either
device. No fresh-create provenance, first-format API or implicit creation is a
runtime prerequisite. Source devices are internal runtime inputs, never
application `volumeDevices`; the application sees the final filesystem.

## Exact external-source contract

The actual outer application PVC supplies same-namespace references:

```yaml
metadata:
  annotations:
    dmstack.csi.infinitydb.io/origin-pvc: existing-origin
    dmstack.csi.infinitydb.io/cache-pvc: existing-cache
```

The controller reads the real outer PVC using CSI Create's PVC name/namespace,
pins its UID and references before provider effects, and publishes annotation
`dmstack.csi.infinitydb.io/external-source-pvcs-v1` on CompositeVolume.
Its exact independent field contract is in
`testdata/external-source-pvcs-v1.json`:

```text
{version:1, owner, config, outer, origin, cache}
config = {namespace,namespaceUID,nodeName,nodeUID,originDriver?,cacheDriver?}
outer = {name,uid}
origin/cache = {name,uid,pvName,pvUID,className,classUID,driver,handle,bytes}
```

Order matches the controller's canonical serialization. `owner` is Composite
**NAME.UID**, never the outer UID. Incomplete owner, unknown versions/fields,
self/same references, namespace/ordinal expansion and old `backing-pvcs-v1`
objects fail closed. No deterministic claim names, source ownership annotations,
special namespace labels, binding-mode provenance, or source finalizers are
required or written.

The independent read-only API client checks the real outer UID and annotations,
outer Filesystem/RWO/requested size, both source Block/RWO bindings, PVC/PV/class
UIDs, exact claimRefs/drivers/handles/capacities and node placement. Existing
source consumers are rejected. Outer annotations are references, not placement
configuration or device evidence. Actual VG/LV/kernel identity is separately
observed by the provider; a name or supplied stable ID never enrolls media.

The optional config driver names default to `disk.csi.azure.com` and
`localdisk.csi.acstor.io`. They are exact lowercase DNS names of at most 63
characters, distinct and not the other role's official alias. Explicit identical
defaults are semantically equal without rewriting API bytes. Per-source
`driver` is always explicit. IDs/handles are never case-folded or normalized.

## Separate canary process, never an addon replacement

Online mode requires independent registration, plugin/journal storage and an
exclusive pre-provisioned nondefault VG:

```text
ACS_DYNAMIC_BLOCK=1 ACS_RAW_CACHE=1
--driver-name=localdisk.dmstack.infinitydb.io
--canary-volume-group=dmstack-canary
--ordinary-journal-directory=/csi-canary/hold
--csi-node-bind-address=unix:///csi-canary/node.sock
--csi-controller-bind-address=unix:///csi-canary/controller.sock
--raw-cache-rpc-port=9711
--node-name=<exact canary node>
```

Give this process its own persistent host plugin mount/registrar/CSIDriver/RBAC/
StorageClass, not the managed addon's paths or identity. Its VG must have tag
`dmstack.csi.localdisk.dmstack.infinitydb.io`, **without** `local-csi`.
The source cache class must explicitly select `volumeGroup: dmstack-canary`.
Actual UUID/tag are checked before serving. Foreign VG/mapper aliases,
automatic disk/VG creation, broad pool cleanup and ephemeral CSI lifecycle are
disabled. Qualify the installed managed addon's tag/device exclusion separately;
a different CSI name alone does not establish physical isolation.

Mount root-owned, non-group/world-writable configuration into the driver only:

`/etc/acs-dmstack-enrollment/domain.json`:

```json
{
  "NodeName": "worker-a", "NodeUID": "<node UID>",
  "Namespace": "applications", "NamespaceUID": "<namespace UID>",
  "CacheClass": "canary-cache", "CacheClassUID": "<class UID>",
  "VolumeGroup": "dmstack-canary", "PoolUUID": "<actual VG UUID>",
  "Bytes": 1073741824,
  "OriginDriver": "disk.dmstack.infinitydb.io",
  "CacheDriver": "localdisk.dmstack.infinitydb.io"
}
```

Cache class/bytes remain administrator *restrictions in this domain
configuration*, compared against the cache role pins; they are not fields in
the external journal config and do not request provisioning.
`reader.kubeconfig` in the same directory must contain one explicit read-only
identity/context, HTTPS with embedded CA, and dedicated token or embedded mTLS.
No ambient/in-cluster fallback, exec/auth plugins, insecure TLS or proxies.
Reader permissions: get CompositeVolumes/nodes/namespaces/PVs/StorageClasses;
get PVCs and list Pods only in the configured namespace. No source write/delete
permission is needed. Restrict resourceNames, network paths and source access
by namespace RBAC/admission; no arbitrary privileged/raw-device consumers.

The existing mTLS provider files remain in `/etc/acs-dmstack-provider`:
`tls.crt`, `tls.key`, `client-ca.crt`, `policy.json`. Policy pins actual
**VG UUID**, exact controller/node certificate URI scopes and `localCustody:true`.
Route directly to this node's endpoint, never a multi-node provider alias.
Wire pool remains **`acs-disposable-block`**, not a driver or class name.
Reserve uses CompositeVolume **NAME**, sealed `spec.cacheParameters.requestID`
and exact cache bytes; it returns RESERVED before node acquisition.

## Durable ownership, independent of API lifetime

Enrollment durably retains the **full canonical verified source snapshot**,
fingerprint, CR incarnation, request binding and observed VG/LV identity.
Ordinary CSI/GC admission participates before enrollment: irreversible native
tombstones and pending provision intents prevent late commands from destroying
newly held media. The candidate/admission intent is saved before probing;
unknown outcomes remain held. Never reset/copy/restore the journal independently
of its media. The persistent parent is pinned before any handlers start.

Fresh admission and data-plane Acquire/Validate/Renew require live, exact
source pins. Committed observation and same-owner cleanup instead use positive
retained metadata **plus actual media identity and independent cleanup proof**,
even if the user deletes API objects. No source finalizers are necessary.
Absence, elapsed TTL or an empty node receipt never releases ownership.
Legacy committed holds retain observation/cleanup evidence; old owned-source
objects cannot enroll under the new reader or be silently rewritten.

ReleaseNode and ReleaseAllocation are distinct:

- ReleaseNode requires exact owner/epoch and independently verified physical
  cleanup. It does not delete sources or automatically close the allocation.
- Controller `Release(ALLOCATION)` needs the exact returned allocation and
  stable request ID, epoch 0 iff never acquired, otherwise the exact verified
  released epoch; node/cleanup fields are empty. It is authorization closure,
  never an LV operation. Preparing/uncertain acquisition is not cancellable
  merely because no success reply arrived.
- Exact committed closure replay and closed-key GetLease are history-only.
  GetLease returns NotFound only for an authoritatively closed key; unknown/
  corrupt state is Unavailable. No late provider Acquire/Reserve may rearm it.
  After closure, normal underlying CSI may again operate on the external PVC;
  that does not reopen provider authorization.

**Remaining single-use limitation:** this canary admits one allocation and
does not support a higher node epoch after verified ReleaseNode. Sources remain
intact, but this adapter does **not** yet qualify normal Pod/StatefulSet restart
reuse. Do not map ordinary ControllerUnpublish to allocation closure to conceal
that limitation. Allocation closure belongs to composite PVC deletion.

## Native host and paired Kata custody

For non-Kata, set `ACS_HOST_CUSTODY=1` and mount
`/etc/acs-dmstack-enrollment/host.json`:

```json
{
  "Socket": "/run/acs-dmstack/host-member.sock",
  "StageRoot": "/var/lib/kubelet/plugins/kubernetes.io/csi/pv",
  "PodsRoot": "/var/lib/kubelet/pods"
}
```

The existing socket parent is root-owned 0700; the real listener is 0600
SOCK_SEQPACKET. Root/host PID/proc/sysfs/device visibility and actual
F_DUPFD_QUERY support are required; unknown capability is not success.
Do not hijack an existing socket. Actual boot/mount identity is pinned and
matched, not trusted from the caller. The node process serves both v1 and the
private `dmstack-host-ofd-v1` member; no additional daemon or guest alias.

The member opens/verifies retained **O_RDWR while a transient O_EXCL probe is
still held**, then releases only the probe. Durable provider exclusion remains.
O_EXCL is not a security fence against ordinary opens, and retaining its kernel
claim would interfere with stock formatter/DM claims. No force flags, patched
formatter, current-FD reopening or first-format service is used.

`retain-host`, `bind-host-current`, `check-host` require current ACQUIRED
ownership, real LV identity and the same retained/current OFD. V1 identity is
bound to the actual verified Acquire client certificate leaf, never caller
fingerprint JSON. Disconnect keeps the original/hold; token-CAS prevents late
disconnect from erasing a successor current. Lost originals are not recreated.

The cleanup-only retain/bind/check variants may attest that already-retained
same owner/boot in ACQUIRED or HELD. They durably latch cleanup-only; no new
use, lost-original reopen, Renew or Validate-held bypass. `finish-host` blocks
future borrows, closes member originals, independently checks actual open counts
(including queued SCM_RIGHTS), holders and mounts, then holds an exclusive probe
through fsynced **native-host-teardown** evidence. Busy/unknown retains ownership.
The trusted facade must additionally drain its own privileged executor and
quarantine uncertain Create/format intents; a zero-reference snapshot does not
prove no orphaned command can act later.

Kata keeps runtime-owned paired projection of origin/cache into the guest and
guest dm-cache/filesystem assembly. Application Pods still see only the final
filesystem. Preserve the existing external custody path and require exact
**guest-clean AND host-unplugged** evidence for each raw provider device;
native-host receipts are not interchangeable. This host member is not a Kata
projection daemon. Bound/Reserve is not attach evidence; the origin provider
and composite ControllerPublish must coordinate its exact normal attachment.

## Validation and preserved experiments

Use native Linux `TMPDIR` and `GOTMPDIR`. Root member tests inspect actual host
namespaces but use only regular files/memfds and real SCM_RIGHTS, never raw
host devices:

```sh
go test -p=2 -count=1 -timeout=120s ./internal/devicelease/... ./internal/csi/node/... ./internal/csi/controller/... ./internal/csi/core/lvm/... ./internal/gc/... ./cmd/driver/...
go build -buildvcs=false -p=2 ./cmd/driver ./cmd/manager
go test -race -p=2 -count=1 -timeout=120s ./internal/devicelease/... ./internal/csi/node/... ./internal/csi/core/lvm/... ./cmd/driver/... -run 'TestHost|TestBacking|TestCanary|TestSeparate|TestRetainedExternal|TestExternalSources|TestAllocationClosure|TestDynamic|TestAuthoritative|TestOrdinary|TestEnrollment|TestRPCResolve'
```

Windows worktree metadata requires `-buildvcs=false` when building under Linux.
Software tests/model-gated guest formatter/DM evidence are not live-provider
identity qualification. No deployment, source provisioning or live disk test
is part of this change.

The superseded owned-backing parser/tests/fixture and previous instructions are
preserved under `archive/owned-backing-pvcs/` (noncompiled `.go.txt` sources).
Earlier raw-pool/offline administrative experiments remain optional historical
code, not prerequisites or references in the active external-source path.
