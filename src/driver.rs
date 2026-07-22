use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use tokio::runtime::Runtime;

use crate::abi::{self, IrodoriConnectorBuffer};
use crate::{ABI_VERSION, CONFIG_JSON, DRIVER_LINKED, ENGINE, MANIFEST_JSON};

static CONNECTIONS: OnceLock<Mutex<HashMap<String, BigQueryConnection>>> = OnceLock::new();
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[derive(Clone)]
struct BigQueryConnection {
    client: Client,
    config: BigQueryConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BigQueryConfig {
    project_id: String,
    access_token: String,
    location: Option<String>,
    redaction_values: Vec<String>,
}

#[derive(Default)]
struct ObjectMeta {
    columns: Vec<Value>,
}

#[derive(Deserialize)]
struct GcpServiceAccountKey {
    project_id: String,
    client_email: String,
    private_key: String,
}

type QueryRows = Vec<Vec<Value>>;
type QueryOutput = (Vec<String>, QueryRows, bool);

fn connections() -> &'static Mutex<HashMap<String, BigQueryConnection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn runtime() -> Result<&'static Runtime, String> {
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let runtime = Runtime::new().map_err(|err| format!("create tokio runtime failed: {err}"))?;
    let _ = RUNTIME.set(runtime);
    RUNTIME
        .get()
        .ok_or_else(|| "create tokio runtime failed.".to_string())
}

pub fn call_json(request: IrodoriConnectorBuffer) -> IrodoriConnectorBuffer {
    let request = match abi::parse_request(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let method = match abi::request_method(request.as_ref()) {
        Ok(method) => method,
        Err(response) => return response,
    };

    match method {
        "health" | "ping" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        ])),
        "describe" | "capabilities" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "manifest".to_string(),
                serde_json::from_str(MANIFEST_JSON).unwrap_or(Value::Null),
            ),
            (
                "config".to_string(),
                serde_json::from_str(CONFIG_JSON).unwrap_or(Value::Null),
            ),
        ])),
        "manifest" => abi::owned_buffer(MANIFEST_JSON.to_string()),
        "config" => abi::owned_buffer(CONFIG_JSON.to_string()),
        "connect" => connect(request.as_ref().expect("connect has request")),
        "query" => query(request.as_ref().expect("query has request")),
        "metadata" => metadata(request.as_ref().expect("metadata has request")),
        "close" => close(request.as_ref().expect("close has request")),
        other => abi::error(
            "connector.unknownMethod",
            format!("unknown connector method: {other}"),
        ),
    }
}

fn connect(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let config = match runtime()
        .and_then(|runtime| runtime.block_on(BigQueryConfig::from_request(request)))
    {
        Ok(config) => config,
        Err(err) => return abi::error("connector.invalidRequest", err),
    };
    let connection = BigQueryConnection {
        client: Client::new(),
        config,
    };
    let dataset_count = match runtime().and_then(|runtime| runtime.block_on(probe(&connection))) {
        Ok(count) => count,
        Err(err) => return abi::error("connector.connectFailed", connection.config.redact(&err)),
    };
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let mut response = Map::from_iter([
        ("engine".to_string(), Value::String(ENGINE.to_string())),
        (
            "connectionId".to_string(),
            Value::String(connection_id.clone()),
        ),
        ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        (
            "projectId".to_string(),
            Value::String(connection.config.project_id.clone()),
        ),
        ("datasetCount".to_string(), json!(dataset_count)),
        (
            "serverVersion".to_string(),
            Value::String("Google BigQuery v2 API".to_string()),
        ),
    ]);
    if let Some(location) = connection.config.location.as_deref() {
        response.insert("location".to_string(), Value::String(location.to_string()));
    }
    guard.insert(connection_id, connection);
    abi::ok(response)
}

fn query(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let Some(sql) = abi::string_field(request, "sql")
        .or_else(|| abi::string_field(request, "query"))
        .or_else(|| abi::string_field(request, "statement"))
    else {
        return abi::error(
            "connector.invalidRequest",
            "query requires a string sql, query, or statement field.",
        );
    };
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime()
        .and_then(|runtime| runtime.block_on(run_query(&connection, sql, abi::max_rows(request))))
    {
        Ok((columns, rows, truncated)) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            (
                "columns".to_string(),
                Value::Array(columns.into_iter().map(Value::String).collect()),
            ),
            (
                "rows".to_string(),
                Value::Array(rows.into_iter().map(Value::Array).collect()),
            ),
            ("truncated".to_string(), Value::Bool(truncated)),
        ])),
        Err(err) => abi::error("connector.queryFailed", connection.config.redact(&err)),
    }
}

fn metadata(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime().and_then(|runtime| runtime.block_on(load_metadata(&connection))) {
        Ok(metadata) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            ("metadata".to_string(), metadata),
        ])),
        Err(err) => abi::error("connector.metadataFailed", connection.config.redact(&err)),
    }
}

fn close(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let existed = guard.remove(&connection_id).is_some();
    abi::ok(Map::from_iter([
        ("connectionId".to_string(), Value::String(connection_id)),
        ("closed".to_string(), Value::Bool(existed)),
    ]))
}

impl BigQueryConfig {
    async fn from_request(request: &Value) -> Result<Self, String> {
        let service_json = option_string(
            request,
            &["serviceAccountJson", "credentialsJson", "serviceAccountKey"],
        )
        .or_else(|| {
            option_string(request, &["password", "privateKey"])
                .filter(|value| value.trim_start().starts_with('{'))
        });
        let (project_id, access_token) = if let Some(service_json) = service_json {
            let key: GcpServiceAccountKey = serde_json::from_str(&service_json)
                .map_err(|err| format!("invalid Google service account JSON: {err}"))?;
            let token =
                fetch_oauth2_token(&Client::new(), &key.client_email, &key.private_key).await?;
            (key.project_id, token)
        } else {
            let project_id =
                option_string(request, &["projectId", "project", "database", "db", "host"])
                    .ok_or_else(|| "BigQuery requires projectId, database, or host.".to_string())?;
            let access_token = option_string(
                request,
                &[
                    "token",
                    "accessToken",
                    "oauthAccessToken",
                    "bearerToken",
                    "password",
                ],
            )
            .or_else(|| std::env::var("GOOGLE_OAUTH_ACCESS_TOKEN").ok())
            .ok_or_else(|| {
                "BigQuery requires an OAuth access token or service account JSON.".to_string()
            })?;
            (project_id, access_token)
        };
        let location = option_string(request, &["location", "region"]);
        let mut redaction_values = Vec::new();
        push_sensitive(&mut redaction_values, Some(&access_token));
        Ok(Self {
            project_id,
            access_token,
            location,
            redaction_values,
        })
    }

    fn redact(&self, message: &str) -> String {
        self.redaction_values
            .iter()
            .fold(message.to_string(), |message, secret| {
                if secret.is_empty() {
                    message
                } else {
                    message.replace(secret, "****")
                }
            })
    }
}

async fn probe(connection: &BigQueryConnection) -> Result<usize, String> {
    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets?maxResults=1",
        connection.config.project_id
    );
    let value = request_json(connection, connection.client.get(url)).await?;
    Ok(value
        .get("datasets")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0))
}

async fn run_query(
    connection: &BigQueryConnection,
    sql: &str,
    cap: usize,
) -> Result<QueryOutput, String> {
    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/queries",
        connection.config.project_id
    );
    let mut payload = json!({
        "query": sql,
        "useLegacySql": false,
        "maxResults": cap.min(10_000),
        "timeoutMs": 30_000
    });
    if let Some(location) = connection.config.location.as_deref() {
        payload["location"] = Value::String(location.to_string());
    }
    let mut value = request_json(connection, connection.client.post(url).json(&payload)).await?;
    if let Some(error) = query_error(&value) {
        return Err(error);
    }
    let job_reference = value.get("jobReference").cloned();
    for _ in 0..120 {
        if value
            .get("jobComplete")
            .and_then(Value::as_bool)
            .unwrap_or(true)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        value = get_query_results(connection, job_reference.as_ref(), None, cap).await?;
        if let Some(error) = query_error(&value) {
            return Err(error);
        }
    }
    let (columns, mut rows, mut truncated, mut page_token) = parse_query_response(&value, cap);
    while rows.len() < cap {
        let Some(token) = page_token.take() else {
            break;
        };
        let next = get_query_results(connection, job_reference.as_ref(), Some(&token), cap).await?;
        if let Some(error) = query_error(&next) {
            return Err(error);
        }
        let (_, next_rows, next_truncated, next_page) =
            parse_query_response(&next, cap - rows.len());
        rows.extend(next_rows);
        truncated |= next_truncated;
        page_token = next_page;
    }
    if page_token.is_some() {
        truncated = true;
    }
    Ok((columns, rows, truncated))
}

async fn get_query_results(
    connection: &BigQueryConnection,
    job_reference: Option<&Value>,
    page_token: Option<&str>,
    cap: usize,
) -> Result<Value, String> {
    let job_id = job_reference
        .and_then(|value| value.get("jobId"))
        .and_then(Value::as_str)
        .ok_or_else(|| "BigQuery response missing jobReference.jobId.".to_string())?;
    let location = job_reference
        .and_then(|value| value.get("location"))
        .and_then(Value::as_str)
        .or(connection.config.location.as_deref());
    let mut url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/queries/{job_id}?maxResults={}",
        connection.config.project_id,
        cap.min(10_000)
    );
    if let Some(location) = location {
        url.push_str("&location=");
        url.push_str(location);
    }
    if let Some(page_token) = page_token {
        url.push_str("&pageToken=");
        url.push_str(page_token);
    }
    request_json(connection, connection.client.get(url)).await
}

fn parse_query_response(
    value: &Value,
    cap: usize,
) -> (Vec<String>, Vec<Vec<Value>>, bool, Option<String>) {
    let columns = value
        .pointer("/schema/fields")
        .and_then(Value::as_array)
        .map(|fields| {
            fields
                .iter()
                .filter_map(|field| field.get("name").and_then(Value::as_str))
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut rows = Vec::new();
    if let Some(rowset) = value.get("rows").and_then(Value::as_array) {
        for row in rowset {
            if rows.len() >= cap {
                break;
            }
            let values = row
                .get("f")
                .and_then(Value::as_array)
                .map(|cells| {
                    cells
                        .iter()
                        .map(|cell| cell.get("v").cloned().unwrap_or(Value::Null))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| vec![Value::Null; columns.len()]);
            rows.push(values);
        }
    }
    let page_token = value
        .get("pageToken")
        .and_then(Value::as_str)
        .map(str::to_string);
    let truncated = page_token.is_some()
        || value
            .get("totalRows")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<usize>().ok())
            .map(|total| total > rows.len())
            .unwrap_or(false);
    (columns, rows, truncated, page_token)
}

fn query_error(value: &Value) -> Option<String> {
    value
        .get("errors")
        .and_then(Value::as_array)
        .and_then(|errors| errors.first())
        .and_then(|error| error.get("message").and_then(Value::as_str))
        .map(str::to_string)
}

async fn load_metadata(connection: &BigQueryConnection) -> Result<Value, String> {
    let datasets_url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets",
        connection.config.project_id
    );
    let value = request_json(connection, connection.client.get(datasets_url)).await?;
    let datasets = value
        .get("datasets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|dataset| {
            dataset
                .pointer("/datasetReference/datasetId")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect::<Vec<_>>();
    let mut schemas: BTreeMap<String, BTreeMap<String, ObjectMeta>> = BTreeMap::new();
    for dataset in datasets {
        schemas.entry(dataset.clone()).or_default();
        let sql = format!(
            "SELECT table_name, column_name, data_type, ordinal_position, is_nullable \
             FROM `{}`.INFORMATION_SCHEMA.COLUMNS \
             ORDER BY table_name, ordinal_position",
            dataset.replace('`', "")
        );
        let Ok((columns, rows, _)) = run_query(connection, &sql, 10_000).await else {
            continue;
        };
        for row in rows {
            let table = field(&columns, &row, "table_name").unwrap_or_default();
            let column = field(&columns, &row, "column_name").unwrap_or_default();
            if table.is_empty() || column.is_empty() {
                continue;
            }
            let object = schemas
                .entry(dataset.clone())
                .or_default()
                .entry(table)
                .or_default();
            object.columns.push(json!({
                "name": column,
                "dataType": field(&columns, &row, "data_type").unwrap_or_default(),
                "nullable": field(&columns, &row, "is_nullable")
                    .map(|value| value.eq_ignore_ascii_case("YES") || value.eq_ignore_ascii_case("true"))
                    .unwrap_or(true),
                "ordinal": field(&columns, &row, "ordinal_position")
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or((object.columns.len() + 1) as i64)
            }));
        }
    }
    Ok(json!({
        "schemas": schemas
            .into_iter()
            .map(|(schema, objects)| json!({
                "name": schema,
                "objects": objects
                    .into_iter()
                    .map(|(name, object)| json!({
                        "schema": schema,
                        "name": name,
                        "kind": "table",
                        "columns": object.columns,
                        "indexes": [],
                        "primaryKey": [],
                        "foreignKeys": []
                    }))
                    .collect::<Vec<_>>()
            }))
            .collect::<Vec<_>>()
    }))
}

async fn request_json(
    connection: &BigQueryConnection,
    builder: reqwest::RequestBuilder,
) -> Result<Value, String> {
    let response = builder
        .bearer_auth(&connection.config.access_token)
        .send()
        .await
        .map_err(|err| format!("BigQuery request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("BigQuery response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!("BigQuery returned HTTP {status}: {text}"));
    }
    serde_json::from_str::<Value>(&text)
        .map_err(|err| format!("BigQuery JSON response parse failed: {err}: {text}"))
}

async fn fetch_oauth2_token(
    client: &Client,
    email: &str,
    private_key: &str,
) -> Result<String, String> {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let exp = now + 3600;
    let header = r#"{"alg":"RS256","typ":"JWT"}"#;
    let claims = format!(
        r#"{{"iss":"{}","scope":"https://www.googleapis.com/auth/bigquery","aud":"https://oauth2.googleapis.com/token","exp":{},"iat":{}}}"#,
        email, exp, now
    );
    let payload = format!(
        "{}.{}",
        base64_url_encode(header.as_bytes()),
        base64_url_encode(claims.as_bytes())
    );
    let signature = sign_rs256(private_key, payload.as_bytes())?;
    let assertion = format!("{payload}.{}", base64_url_encode(&signature));
    let body = format!(
        "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion={assertion}"
    );
    let response = client
        .post("https://oauth2.googleapis.com/token")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .map_err(|err| format!("GCP token request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("GCP token response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!("GCP token request returned HTTP {status}: {text}"));
    }
    let value = serde_json::from_str::<Value>(&text)
        .map_err(|err| format!("GCP token JSON parse failed: {err}: {text}"))?;
    value
        .get("access_token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| "GCP token response missing access_token.".to_string())
}

fn sign_rs256(private_key: &str, message: &[u8]) -> Result<Vec<u8>, String> {
    use ring::rand::SystemRandom;
    use ring::signature::{RsaKeyPair, RSA_PKCS1_SHA256};

    let key = pem::parse(private_key)
        .map_err(|_| "invalid Google service account private key PEM.".to_string())?;
    if key.tag() != "PRIVATE KEY" {
        return Err("Google service account private key must use PKCS#8 PEM.".to_string());
    }
    let key_pair = RsaKeyPair::from_pkcs8(key.contents())
        .map_err(|_| "invalid Google service account PKCS#8 private key.".to_string())?;
    let mut signature = vec![0; key_pair.public().modulus_len()];
    key_pair
        .sign(
            &RSA_PKCS1_SHA256,
            &SystemRandom::new(),
            message,
            &mut signature,
        )
        .map_err(|_| "Google service account JWT signing failed.".to_string())?;
    Ok(signature)
}

fn base64_url_encode(input: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    let mut i = 0;
    while i < input.len() {
        let b0 = input[i] as usize;
        let b1 = if i + 1 < input.len() {
            input[i + 1] as usize
        } else {
            0
        };
        let b2 = if i + 2 < input.len() {
            input[i + 2] as usize
        } else {
            0
        };
        out.push(CHARS[b0 >> 2] as char);
        out.push(CHARS[((b0 & 3) << 4) | (b1 >> 4)] as char);
        if i + 1 < input.len() {
            out.push(CHARS[((b1 & 15) << 2) | (b2 >> 6)] as char);
        }
        if i + 2 < input.len() {
            out.push(CHARS[b2 & 63] as char);
        }
        i += 3;
    }
    out
}

fn connection(connection_id: &str) -> Result<BigQueryConnection, IrodoriConnectorBuffer> {
    let guard = connections().lock().map_err(|_| {
        abi::error(
            "connector.statePoisoned",
            "Connector connection state is poisoned.",
        )
    })?;
    guard.get(connection_id).cloned().ok_or_else(|| {
        abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        )
    })
}

fn field(columns: &[String], row: &[Value], name: &str) -> Option<String> {
    columns
        .iter()
        .position(|column| column.eq_ignore_ascii_case(name))
        .and_then(|index| row.get(index))
        .and_then(|value| match value {
            Value::Null => None,
            Value::String(value) => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            Value::Bool(value) => Some(value.to_string()),
            _ => None,
        })
}

fn request_containers(request: &Value) -> Vec<&Value> {
    [
        Some(request),
        request.get("profile"),
        request.get("options"),
        request.get("auth"),
        request.get("secrets"),
        request
            .get("profile")
            .and_then(|profile| profile.get("options")),
        request
            .get("profile")
            .and_then(|profile| profile.get("auth")),
        request
            .get("profile")
            .and_then(|profile| profile.get("secrets")),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn option_string(request: &Value, fields: &[&str]) -> Option<String> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields.iter().find_map(|field| {
                container
                    .get(*field)
                    .map(|value| match value {
                        Value::String(value) => value.clone(),
                        Value::Number(value) => value.to_string(),
                        Value::Bool(value) => value.to_string(),
                        _ => String::new(),
                    })
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
        })
}

fn push_sensitive(values: &mut Vec<String>, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        if !values.iter().any(|existing| existing == value) {
            values.push(value.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_base64_url_without_padding() {
        assert_eq!(base64_url_encode(b"abc"), "YWJj");
        assert_eq!(base64_url_encode(b"ab"), "YWI");
    }

    #[test]
    fn parses_token_config() {
        let request = json!({
            "profile": {
                "projectId": "project-a",
                "token": "ya29.token",
                "location": "US"
            }
        });
        let runtime = Runtime::new().unwrap();
        let config = runtime
            .block_on(BigQueryConfig::from_request(&request))
            .unwrap();
        assert_eq!(config.project_id, "project-a");
        assert_eq!(config.location.as_deref(), Some("US"));
    }
}
