# Trustless Gateway request shape vs. per-block plan

## Findings
- Spec scope: In the Trustless Gateway spec §2.2.2, `dag-scope` is “only available for CAR requests”; §5 lists `dag-scope`/`entity-bytes` as CAR parameters. Raw block responses (§4) have no traversal params.
- Raw block shape: Spec signals raw via `Accept: application/vnd.ipld.raw` or `?format=raw`. `boxo` remote blockstore follows this (`/ipfs/<cid>?format=raw`, Accept raw, no `dag-scope`).
- CAR shape: Traversal parameters (`dag-scope`, `entity-bytes`) pair with CAR responses signaled via `Accept: application/vnd.ipld.car` or `?format=car`.
- Implication for plan: The plan’s per-block fetch (`/ipfs/{cid}?dag-scope=block` + Accept raw) is outside the spec; deployed gateways likely ignore or reject `dag-scope` on raw. For broad compatibility, use raw without `dag-scope`, or switch to CAR with `dag-scope=block` and parse CAR.
