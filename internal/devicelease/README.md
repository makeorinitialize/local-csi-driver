# Node-local raw cache reservations

## Ordinary ACS Block PV canary (no allocation)

This mode uses **one already provisioned, disposable, normal ACS Block PV
on its existing node**. It does not create, format, activate, erase, resize,
rename, or remove that LV. It does not require the dedicated VG below.

1. Verify the PV/PVC, `volumeMode: Block`, CSI driver, node, existing
   `VG#pvc-<uuid>` handle, actual byte count, and VG/LV UUIDs independently.
   The trusted operator attests the PV is disposable and not shared. The
   canary accepts an active healthy normal or RAID0 LV in a local `local-csi`
   tagged VG; no tagged/quarantined LV, device path or pod annotation is
   an enrollment. **Provision the PV but leave it unpublished and unconsumed.**
   Do not stage or publish the backing PVC to a helper, app or other pod.
   A kubelet bind of a raw block inode can remain even when LVM reports no
   opener. If already published, complete normal unpublish/unstage and verify
   the host is clear before attempting enrollment.
2. Stop all ACS writers on **that node** during enrollment. Use this binary
   offline with the same persistent `/csi-node` plugin-directory mount,
   device inventory, `NODE_NAME`, `ACS_RAW_CACHE=1`, and administrator
   privileges: `/local-csi-driver raw-cache enroll-block < enrollment.json`.
   Do not run `raw-cache init`. Enrollment input is one object:

   ```json
   {
     "NodeID": "worker-a",
     "CompositeID": "canary-a",
     "VolumeHandle": "containerstorage#pvc-00000000-0000-0000-0000-000000000001",
     "VolumeMode": "Block",
     "VGUUID": "<observed vg_uuid>",
     "LVUUID": "<observed lv_uuid>",
     "Generation": 1,
     "Bytes": 1073741824
   }
   ```

   The generation is pinned in the single-use journal, not written to LVM.
   Use one incarnation only; do not remove/recreate the journal, replace
   media, or restore either independently. Enrollment writes and syncs a
   private sibling, publishes it with Linux `RENAME_NOREPLACE`, then syncs
   the parent. A crash before publication may leave an ignored sibling. An
   existing final directory, even an empty or corrupt one, is not repaired.
   Use a local filesystem supporting these operations. Upgraded drivers pin the
   plugin directory while running so enrollment cannot race an instance
   that started without the guards. Older binaries do not participate.
3. Restart only the protecting binary. Use the existing two RPC opt-ins,
   node-specific mTLS endpoint, `localCustody: true`, exact node/composite
   grants and independent custodian described below. The unmodified chart
   still requires its configured host directories. An ordinary enrollment
   selects the canary backend exclusively on that node: wire `provider` is
   the **VG UUID**, `pool_id` is `acs-disposable-block`. `Reserve` resolves
   and durably holds the enrolled LV **only for exactly its observed byte
   count**. Advisory capacity is this single slot's size when available,
   zero when known busy, and an error when unknown. Busy, size mismatch
   and inconclusive first admission do not create a request binding or hold
   (a failure syncing a dispatched commit still requires exact replay).
   The opaque `resource_id` is `VG/LV`; preserve
   case and the exact VG/LV UUIDs. No host paths are accepted from the RPC.
4. `Acquire` checks node, allocation, generation, epoch, custodian enrollment,
   no holders/opens and an exclusive identity-checked probe, without writing
   media. Enrollment and first `Reserve` also check host PID 1's kubelet
   raw publish mounts (standard `/var/lib/kubelet` layout and host PID
   namespace required). Reserve keeps the verified exclusive FD open
   through journal sync under the same lock as ACS teardown. This is not a
   continuous kernel fence. **Before enrollment**, isolate the node from
   other consumers: only the trusted custodian may grant future device
   access. It must prevent *all* unauthorized opens, device-node copies,
   alternate-namespace mounts and raw writers until handoff/teardown.
   ACS does not keep an exclusive FD across RPCs. `Validate` requires
   the actual open file description from the custodian. Losing liveness
   does not free the volume.
5. To release the node, the custodian must independently confirm **both**
   `guest-clean` and `host-unplugged` for the *same* immutable receipt and
   exact owner. The latter must exclude any further IO/reopen by that guest.
   ACS rechecks the live identity and absence of holders and takes another
   exclusive probe. It commits retirement without deactivation. Stale
   receipts, uncertain inventory, unavailable custodian or open devices
   leave ordinary teardown blocked. If resolution succeeded but no node
   ever acquired, there is intentionally no automatic unlock.

The same flock and fsynced journal serialize enrollment/resolve/hold with
CSI unpublish, unstage, delete (including pre-delete unmount), recovery,
expand, orphan/failover GC, quarantine, wipe reaper and pool cleanup. This
driver's unstage is normally a no-op; it still cannot acknowledge unstage
while held. A conflicting operation that wins *before* resolve durably
retires the enrollment before invoking the mounter/LVM; resolving later is
impossible even after restart. Once resolved, those operations fail until
verified release. This is intentionally single-use, not a way to suspend
and re-enable arbitrary PVCs. The version-3 journal stays in
`/csi-node/disposable-block-hold` and is checked even when RPC is disabled.
For unpublish with an unrelated handle, a missing target DM UUID alone
is not permission to clean a block inode. ACS verifies the enrolled LV's
live LVM and kernel identity and distinct device number against its journal,
or requires durable retirement. Uncertain or conflicting evidence blocks
cleanup; a successful unrelated cleanup does not change the enrolled hold.

**No online downgrade** with an outstanding hold. Legacy binaries, external
LVM/root writers, kubelet plugin-directory removal or replacement, and an
untrusted custodian are outside this interlock. Quiesce and isolate before
rollback; disabling a chart switch is not proof of guest cleanup. The guest
agent, host hot-unplug implementation and custodian are not installed here.

## Dedicated raw-cache pool (separate mode)

This is the allocation portion of a raw cache provider, not a second PVC,
CSI capability or attachment service. It supports administrator `init`,
`reserve` and `inspect`, and optionally the DeviceProvider v1 reservation
RPCs. By default none returns a device path, activates an LV or grants IO.

## Opt in

- Deploy a driver image containing these changes. Before upgrade, verify
  that no StorageClass, static PV, RAID configuration or existing ordinary VG
  uses the newly reserved name `acs-dmstack-raw-v1`.
- For each participating node, provision a **dedicated, local, non-shared**
  VG of that name with the sole tag `acs.dmstack.raw.pool.v1`. Use storage
  excluded from ordinary disk provisioning and RAID setup. It must start
  empty, with no other writers or automatic activation. The provider does
  not discover disks, initialize PVs, create VGs or adopt existing LVs.
- Pre-provision `/var/lib/acs-dmstack-raw` on the node: root-owned, mode
  `0700`, on persistent local storage supporting `flock`, atomic rename and
  directory fsync. Keep one directory for the lifetime of the pool. Do not
  share, replace, clone or independently restore the directory or VG. The
  dedicated VG UUID identifies this local provider instance. This is not a
  cluster-wide composite ID or request ledger.
- `rawCache.enabled: true` mounts that directory into the driver container
  only and enables its root-only local administrative command. Nodes selected
  by that DaemonSet must all have the directory. Enabling the value alone
  neither initializes the journal nor allocates space. There is no Service,
  socket, new credential, RBAC rule or workload-facing parameter.

In the enabled driver container the command forms are:

```text
/local-csi-driver raw-cache init
/local-csi-driver raw-cache reserve REQUEST_ID COMPOSITE_ID BYTES
/local-csi-driver raw-cache inspect POOL_UUID LEASE_ID
```

`init` is a one-time bootstrap, **never** a journal repair procedure.
Reserve writes a versioned request binding and an incarnation intent before
issuing a single tagged, inactive, activation-skipped linear `lvcreate`.
Space rounds up to whole extents. A positive result requires the real
VG and LV inventory to agree with the ledger, including actual capacity,
LV UUID, lease and generation tags. Compatible requests converge; a request
key cannot change inputs, even after an uncertain command. After a lost
reply, retry the same input. A visible matching intent can finish without a
second create; an absent dispatched LV blocks the pool rather than risking a
second allocation. Preserve evidence and escalate if this persists.

Ledger access is serialized across local processes. Validation is linear in
pool PVs, LVs and journal size; mutations rewrite and sync a snapshot. There
is no polling loop, compaction or per-PVC API lookup. Use is limited to
low-rate reservations. Retain the journal and extents indefinitely.

## Facade wiring

The gRPC service `dmstack.provider.v1.DeviceProvider` is registered only on
a separate TLS 1.3 mutual-auth listener. No new method is registered on a CSI
socket. The checked-in `v1` schema and generated Go bindings can be rebuilt
with `sh internal/devicelease/v1/generate.sh` (libprotoc 3.12.4, pinned
public Go plugins). No private Go dependency is required.

1. Complete local pool bootstrap above. The wire `provider` is **the
   dedicated VG UUID** (also `PoolUUID` in the operator receipt), and
   `pool_id` is **`acs-dmstack-raw-v1`**. Do not assign multiple nodes the
   same provider identity.
2. Provision `/etc/acs-dmstack-provider` on each serving node. Protect it
   from workload access and non-admin writes. Include `tls.crt` (server
   certificate chain), `tls.key` (private server key), `client-ca.crt`
   (dedicated client trust roots), and `policy.json`. Example policy:

   ```json
   {
     "provider": "<dedicated VG UUID>",
     "grants": [{
       "uri": "spiffe://storage.example/facade/controller",
       "nodeID": "worker-a",
       "role": "controller",
       "composites": ["example-composite"]
     }]
   }
   ```

   `nodeID` must match the driver's actual Kubernetes node. Match exactly
   one URI SAN in the verified client leaf; do not rely on CN, headers or
   client-supplied node identity. A controller can reserve and read; `node`
   can only read. `composites` is an exact allowlist. Use `allComposites:
   true` instead **only** for a principal trusted for the entire local pool.
3. Set **both** `rawCache.enabled` and `rawCache.rpc.enabled` to `true`.
   Port defaults to `9711` (`rawCache.rpc.port`). No Service, hostPort or
   certificate controller is installed. Route the facade directly to the
   specific provider pod, not a load-balanced set of node-local ledgers.
   Configure a gRPC TLS target such as `worker-a-provider.example:9711`,
   trusted server CA, matching server DNS SAN, and client cert/key with the
   authorized URI. Restrict ingress to intended clients. Missing or invalid
   identity files prevent listener startup; they never enable plaintext.
   Replace certificates, trust roots or policy via a controlled pod restart;
   there is no hot reload. Client chain expiration is checked per call.
4. `GetCapacity` reports reservable linear extents from the reconciled
   VG/PV/LV inventory. `Reserve` requires a nonempty stable request ID,
   composite ID and positive byte count. Every replay is reauthorized.
   `GetLease` returns a `RESERVED` lease only for a matching committed
   receipt and current inventory. The resource ID is opaque, not a device
   path. Unknown state is `UNAVAILABLE`, not `NOT_FOUND` or zero capacity.

## Optional local handoff

Set `"localCustody": true` in the node's RPC policy **only** with a separate
trusted custodian at `/etc/acs-dmstack-provider/custody.sock`. No custodian
is installed by ACS. Without one, no IO handoff or cleanup can succeed.
Only the existing, authenticated `node` role for the exact node/composite may
acquire, validate, renew or release its node claim. The allocation remains
charged after release. Allocation destruction/reuse is still unavailable.

The custodian must control **all** raw access on this dedicated, non-shared
node. Local root, arbitrary privileged pods, VG aliases/clones, externally
modified device tables or untrusted code able to replace the socket invalidate
this assumption. There is no cross-node fencing or dependency on a remote
owner. Keep the node clock trustworthy. This is not a storage transport.

ACS connects to the root-owned custodian process over `SOCK_SEQPACKET`
(`SO_PEERCRED`), one bounded request per connection. Each request is a JSON
object with `Version: 1`, a fresh hex `Nonce`, `Operation`, `Receipt` and
`Owner`. The owner fields are `PoolUUID`, `CompositeID`, `LeaseID`,
`ResourceID`, `StableDeviceID`, `NodeID`, `Generation` and `Epoch`.
Reply with `{"Challenge": <exact request>, "Confirmed": true}` **only**
after independently proving the following; never just echo a facade request:

- `enrolled`: access is exclusively mediated by this custodian on this
  node, for this LV incarnation and owner epoch; no prior writer can open
  it. Before the first grant, prevent clients opening the mapping while
  ACS activates, inspects and zeroes it.
- `opened`: the authorized facade has the device exclusively open for this
  owner. Include **one `SCM_RIGHTS` duplicate of that open description**,
  retaining the original. ACS checks block type, dev_t, kernel DM UUID,
  capacity and the enrolled generation against current LVM inventory and
  `Validate.opened`. No device path from the caller is opened.
- `closed`: independently resolve the supplied immutable receipt ID for
  completed teardown of this exact owner. All its users, mappings and
  descriptor copies are gone and access cannot reopen for that epoch.
  Do not attach descriptors for `enrolled` or `closed`. The custodian
  must not call back into the provider while it holds the local journal lock.

The first grant takes the journal lock, persists the owner **before**
activation, verifies the mapping, and zeroes and flushes the entire LV via
an exclusive, identity-checked FD. This can take minutes and serializes
other local provider operations. Later epochs on the same allocation do
not wipe its data. Renew before the returned one-minute deadline; expiry
retains the owner and never deactivates the mapping. A crash during first
preparation intentionally strands that intent for operator reconciliation,
rather than repeating a potentially destructive zero. A lost committed
reply is replayable without activation or wiping.

Node release also requires zero opens, no sysfs holders, a UUID-selected
non-forced deactivation, matching inactive LV metadata and no matching
kernel DM UUID. On ambiguity retain ownership. Retry the identical release
after restoring evidence; replay of a committed release only acknowledges
history, never touches a successor. Do not discard an uncertain journal.

## Rollback

The first admitted owner mutation upgrades the local journal to version 2.
Fresh IDs rejected before an owner transition (for example a stale generation
or epoch) are not journaled. Admission binds the ID and input in the same
durable snapshot as the transition, before any activation or deactivation.
Reservation-only binaries cannot read it. Turning off `localCustody` blocks
new owner operations; it does not clean up existing owners or migrate the
journal. Preserve the journal and assets, including for interrupted handoffs.

CSI/GC protections remain active when `rawCache.enabled` is switched off.
The directory is then unmounted, not erased. Roll out the protecting binary
on all participating nodes **before** enabling reservations. Older binaries
(including the local v0.4.1 release baseline) lack the reserved-VG guards,
including on direct CSI handles and shutdown PV cleanup. **Do not roll a
node back with provider media visible.** Disabling the chart switch is not
a downgrade procedure. Preserve and isolate both media and journal first;
there is no automatic migration or safe online downgrade/reclaim.
