# BigQuery Connector

Adds BigQuery connectivity as an installable connector extension.

This connector is listed in the public Irodori extension marketplace.

## Connector

- Extension ID: `irodori.bigquery`
- Engine ID: `bigquery`
- Wire: `bigquery`
- Default port: `443`
- Native ABI: `irodori.connector.native.v1`

Connector metadata lives in `connector.config.json` and `irodori.extension.json`.
The Rust code only exports the native ABI and embedded JSON so connector metadata
can be customized without code edits.

## Development

```sh
cargo test
make build
```

Release packages place platform-specific native artifacts under `dist/native`.
