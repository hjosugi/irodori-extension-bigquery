# Native Source

The initial source snapshot was copied from `db/bigquery.rs` in the desktop app.

Source SHA-256: `b950c8bc939a571335e338ada86ed96aa51a50a8e8d55ee36b6047958afb6573`.


This directory is a migration staging area for `irodori.bigquery`. The active native
ABI shim lives in `src/lib.rs`; engine-specific connect/query/metadata behavior
should move here as the connector runtime contract is wired into the desktop app.

Engine status from `knowledge/engines.json`: `wired`.
