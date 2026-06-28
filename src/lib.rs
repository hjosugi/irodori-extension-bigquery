//! Native connector ABI for BigQuery.
//!
//! Connector behavior is declared in ../connector.config.json and
//! ../irodori.extension.json so packaging can customize metadata without
//! changing Rust code.

const ABI_VERSION: u32 = 1;
const ENGINE: &str = "bigquery";
const CONFIG_JSON: &str = include_str!("../connector.config.json");
const MANIFEST_JSON: &str = include_str!("../irodori.extension.json");
const NOT_LINKED_RESPONSE_JSON: &str = r#"{"ok":false,"error":{"code":"connector.driverNotLinked","message":"The native connector metadata is available, but the engine-specific driver entrypoint is not linked in this package yet."}}"#;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct IrodoriConnectorBuffer {
    pub ptr: *const u8,
    pub len: usize,
}

fn static_buffer(value: &'static str) -> IrodoriConnectorBuffer {
    IrodoriConnectorBuffer {
        ptr: value.as_ptr(),
        len: value.len(),
    }
}

#[no_mangle]
pub extern "C" fn irodori_extension_abi_version() -> u32 {
    ABI_VERSION
}

#[no_mangle]
pub extern "C" fn irodori_connector_engine_json() -> IrodoriConnectorBuffer {
    static_buffer(ENGINE)
}

#[no_mangle]
pub extern "C" fn irodori_extension_manifest_json() -> IrodoriConnectorBuffer {
    static_buffer(MANIFEST_JSON)
}

#[no_mangle]
pub extern "C" fn irodori_connector_config_json() -> IrodoriConnectorBuffer {
    static_buffer(CONFIG_JSON)
}

#[no_mangle]
pub extern "C" fn irodori_connector_call_json(
    _request: IrodoriConnectorBuffer,
) -> IrodoriConnectorBuffer {
    static_buffer(NOT_LINKED_RESPONSE_JSON)
}

#[no_mangle]
pub extern "C" fn irodori_connector_free_buffer(_buffer: IrodoriConnectorBuffer) {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn manifest_and_config_describe_the_same_connector() {
        let manifest: Value = serde_json::from_str(MANIFEST_JSON).unwrap();
        let config: Value = serde_json::from_str(CONFIG_JSON).unwrap();
        let connector = &manifest["contributes"]["connectors"][0];

        assert_eq!(manifest["id"], config["extensionId"]);
        assert_eq!(connector["engine"], ENGINE);
        assert_eq!(connector["engine"], config["connector"]["engine"]);
        assert_eq!(connector["module"], config["connector"]["module"]);
        assert!(manifest["permissions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|permission| permission == "connectors"));
    }

    #[test]
    fn abi_exports_static_json() {
        assert_eq!(irodori_extension_abi_version(), ABI_VERSION);
        assert!(irodori_extension_manifest_json().len > 0);
        assert!(irodori_connector_config_json().len > 0);
        assert_eq!(irodori_connector_engine_json().len, ENGINE.len());
    }
}
