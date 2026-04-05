use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub status: String,
    pub code: String,
    pub description: String,
}

#[derive(Debug, Serialize)]
pub struct SuccessResponse {
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct MessageOut {
    #[serde(rename = "messageId")]
    pub message_id: String,
    pub body: String,
    pub sender: String,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
}

#[derive(Debug, Serialize)]
pub struct ListMessagesResponse {
    pub status: String,
    pub messages: Vec<MessageOut>,
}

#[derive(Debug, Serialize)]
pub struct SendMessageResult {
    pub recipient: String,
    #[serde(rename = "messageId")]
    pub message_id: String,
}

#[derive(Debug, Serialize)]
pub struct SendMessageResponse {
    pub status: String,
    pub message: String,
    pub results: Vec<SendMessageResult>,
}

#[derive(Debug, Serialize)]
pub struct DeviceOut {
    pub id: i64,
    #[serde(rename = "deviceId", skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    #[serde(rename = "fcmToken")]
    pub fcm_token: String,
    pub active: bool,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    #[serde(rename = "lastUsed")]
    pub last_used: String,
}

#[derive(Debug, Serialize)]
pub struct ListDevicesResponse {
    pub status: String,
    pub devices: Vec<DeviceOut>,
}

#[derive(Debug, Serialize)]
pub struct RegisterDeviceResponse {
    pub status: String,
    pub message: String,
    #[serde(rename = "deviceId")]
    pub device_id: i64,
}

#[derive(Debug, Serialize)]
pub struct SetPermissionResponse {
    pub status: String,
    pub description: String,
}

#[derive(Debug, Serialize)]
pub struct PermissionDetail {
    pub sender: Option<String>,
    #[serde(rename = "messageBox")]
    pub message_box: String,
    #[serde(rename = "recipientFee")]
    pub recipient_fee: i64,
    pub status: String,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
}

#[derive(Debug, Serialize)]
pub struct GetPermissionResponse {
    pub status: String,
    pub description: String,
    pub permission: Option<PermissionDetail>,
}

#[derive(Debug, Serialize)]
pub struct PermissionDetailList {
    pub sender: Option<String>,
    pub message_box: String,
    pub recipient_fee: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Serialize)]
pub struct ListPermissionsResponse {
    pub status: String,
    pub permissions: Vec<PermissionDetailList>,
    #[serde(rename = "totalCount")]
    pub total_count: i64,
}

#[derive(Debug, Serialize)]
pub struct QuoteSingle {
    #[serde(rename = "deliveryFee")]
    pub delivery_fee: i64,
    #[serde(rename = "recipientFee")]
    pub recipient_fee: i64,
}

#[derive(Debug, Serialize)]
pub struct QuoteSingleResponse {
    pub status: String,
    pub description: String,
    pub quote: QuoteSingle,
}

#[derive(Debug, Serialize)]
pub struct QuoteEntry {
    pub recipient: String,
    #[serde(rename = "messageBox")]
    pub message_box: String,
    #[serde(rename = "deliveryFee")]
    pub delivery_fee: i64,
    #[serde(rename = "recipientFee")]
    pub recipient_fee: i64,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct QuoteTotals {
    #[serde(rename = "deliveryFees")]
    pub delivery_fees: i64,
    #[serde(rename = "recipientFees")]
    pub recipient_fees: i64,
    #[serde(rename = "totalForPayableRecipients")]
    pub total_for_payable_recipients: i64,
}

#[derive(Debug, Serialize)]
pub struct QuoteMultiResponse {
    pub status: String,
    pub description: String,
    #[serde(rename = "quotesByRecipient")]
    pub quotes_by_recipient: Vec<QuoteEntry>,
    pub totals: QuoteTotals,
    #[serde(rename = "blockedRecipients")]
    pub blocked_recipients: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct DeliveryBlockedError {
    pub status: String,
    pub code: String,
    pub description: String,
    #[serde(rename = "blockedRecipients")]
    pub blocked_recipients: Vec<String>,
}
