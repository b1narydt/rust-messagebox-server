use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct Payment {
    pub tx: Option<Vec<u8>>,
    pub outputs: Option<Vec<PaymentOutput>>,
    pub description: Option<String>,
    pub labels: Option<Vec<String>>,
    #[serde(rename = "seekPermission")]
    pub seek_permission: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentOutput {
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    pub protocol: Option<String>,
    #[serde(rename = "paymentRemittance")]
    pub payment_remittance: Option<PaymentRemittance>,
    #[serde(rename = "insertionRemittance")]
    pub insertion_remittance: Option<InsertionRemittance>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentRemittance {
    #[serde(rename = "derivationPrefix")]
    pub derivation_prefix: Option<String>,
    #[serde(rename = "derivationSuffix")]
    pub derivation_suffix: Option<String>,
    #[serde(rename = "senderIdentityKey")]
    pub sender_identity_key: Option<String>,
    #[serde(rename = "customInstructions")]
    pub custom_instructions: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertionRemittance {
    pub basket: Option<String>,
    #[serde(rename = "customInstructions")]
    pub custom_instructions: Option<serde_json::Value>,
    pub tags: Option<Vec<String>>,
}
