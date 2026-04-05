use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct SendMessageRequest {
    pub message: Option<SendMessageBody>,
    pub payment: Option<Payment>,
}

#[derive(Debug, Deserialize)]
pub struct SendMessageBody {
    pub recipient: Option<serde_json::Value>,
    pub recipients: Option<serde_json::Value>,
    #[serde(rename = "messageBox")]
    pub message_box: Option<String>,
    #[serde(rename = "messageId")]
    pub message_id: Option<serde_json::Value>,
    pub body: Option<serde_json::Value>,
}

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

#[derive(Debug, Deserialize)]
pub struct ListMessagesRequest {
    #[serde(rename = "messageBox")]
    pub message_box: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AcknowledgeMessageRequest {
    #[serde(rename = "messageIds")]
    pub message_ids: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct RegisterDeviceRequest {
    #[serde(rename = "fcmToken")]
    pub fcm_token: Option<String>,
    #[serde(rename = "deviceId")]
    pub device_id: Option<String>,
    pub platform: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SetPermissionRequest {
    pub sender: Option<String>,
    #[serde(rename = "messageBox")]
    pub message_box: Option<String>,
    #[serde(rename = "recipientFee")]
    pub recipient_fee: Option<serde_json::Value>,
}
