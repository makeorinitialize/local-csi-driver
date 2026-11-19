# Node-local raw cache reservations

This is the allocation portion of a raw cache provider, not a second PVC,
CSI capability or attachment service. It supports administrator `init`,
`reserve` and `inspect`, and optionally the DeviceProvider v1 reservation
RPCs. None returns a device path, activates an LV or grants IO.

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

## Blocked operations and rollback

`Acquire`, `Validate`, `Renew` and `Release` cannot succeed, including on
replay. In-scope callers receive `UNAVAILABLE`. There is no node claim,
opened-device validation, renewal, release, wipe or reuse. A raw LV may contain
old data. Do not activate it or expose it to a workload. A production grant
requires independent exclusive-access authority, sanitation and opened-device
attestation. Releasing a node would additionally require authenticated,
immutable teardown evidence for the exact allocation, node and epoch, plus
proof that the former owner can no longer issue IO. Elapsed time, an absent
mount, a caller-provided string or a momentary zero open count cannot do that.
Nothing in this implementation turns uncertainty or expiry into teardown.

CSI/GC protections remain active when `rawCache.enabled` is switched off.
The directory is then unmounted, not erased. Roll out the protecting binary
on all participating nodes **before** enabling reservations. Older binaries
(including the local v0.4.1 release baseline) lack the reserved-VG guards,
including on direct CSI handles and shutdown PV cleanup. **Do not roll a
node back with provider media visible.** Disabling the chart switch is not
a downgrade procedure. Preserve and isolate both media and journal first;
there is no automatic migration or safe online downgrade/reclaim.
