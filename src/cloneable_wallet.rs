//! Clone-able wrapper around `bsv::wallet::proto_wallet::ProtoWallet`.
//!
//! The BSV SDK's `ProtoWallet` does not derive `Clone`, but `MessageBoxClient`
//! requires `W: WalletInterface + Clone`.  We wrap it in an `Arc` and delegate
//! every `WalletInterface` method, exactly as the mpc-backend does for
//! `HttpWalletJson`.

use std::sync::Arc;

use bsv::wallet::error::WalletError;
use bsv::wallet::interfaces::*;
use bsv::wallet::proto_wallet::ProtoWallet;

/// `Arc`-based Clone wrapper for `ProtoWallet`.
#[derive(Clone)]
pub struct CloneableProtoWallet(pub Arc<ProtoWallet>);

#[async_trait::async_trait]
impl WalletInterface for CloneableProtoWallet {
    async fn create_action(&self, a: CreateActionArgs, o: Option<&str>) -> Result<CreateActionResult, WalletError> { self.0.create_action(a, o).await }
    async fn sign_action(&self, a: SignActionArgs, o: Option<&str>) -> Result<SignActionResult, WalletError> { self.0.sign_action(a, o).await }
    async fn abort_action(&self, a: AbortActionArgs, o: Option<&str>) -> Result<AbortActionResult, WalletError> { self.0.abort_action(a, o).await }
    async fn list_actions(&self, a: ListActionsArgs, o: Option<&str>) -> Result<ListActionsResult, WalletError> { self.0.list_actions(a, o).await }
    async fn internalize_action(&self, a: InternalizeActionArgs, o: Option<&str>) -> Result<InternalizeActionResult, WalletError> { self.0.internalize_action(a, o).await }
    async fn list_outputs(&self, a: ListOutputsArgs, o: Option<&str>) -> Result<ListOutputsResult, WalletError> { self.0.list_outputs(a, o).await }
    async fn relinquish_output(&self, a: RelinquishOutputArgs, o: Option<&str>) -> Result<RelinquishOutputResult, WalletError> { self.0.relinquish_output(a, o).await }
    async fn get_public_key(&self, a: GetPublicKeyArgs, o: Option<&str>) -> Result<GetPublicKeyResult, WalletError> { self.0.get_public_key(a, o).await }
    async fn reveal_counterparty_key_linkage(&self, a: RevealCounterpartyKeyLinkageArgs, o: Option<&str>) -> Result<RevealCounterpartyKeyLinkageResult, WalletError> { self.0.reveal_counterparty_key_linkage(a, o).await }
    async fn reveal_specific_key_linkage(&self, a: RevealSpecificKeyLinkageArgs, o: Option<&str>) -> Result<RevealSpecificKeyLinkageResult, WalletError> { self.0.reveal_specific_key_linkage(a, o).await }
    async fn encrypt(&self, a: EncryptArgs, o: Option<&str>) -> Result<EncryptResult, WalletError> { self.0.encrypt(a, o).await }
    async fn decrypt(&self, a: DecryptArgs, o: Option<&str>) -> Result<DecryptResult, WalletError> { self.0.decrypt(a, o).await }
    async fn create_hmac(&self, a: CreateHmacArgs, o: Option<&str>) -> Result<CreateHmacResult, WalletError> { self.0.create_hmac(a, o).await }
    async fn verify_hmac(&self, a: VerifyHmacArgs, o: Option<&str>) -> Result<VerifyHmacResult, WalletError> { self.0.verify_hmac(a, o).await }
    async fn create_signature(&self, a: CreateSignatureArgs, o: Option<&str>) -> Result<CreateSignatureResult, WalletError> { self.0.create_signature(a, o).await }
    async fn verify_signature(&self, a: VerifySignatureArgs, o: Option<&str>) -> Result<VerifySignatureResult, WalletError> { self.0.verify_signature(a, o).await }
    async fn acquire_certificate(&self, a: AcquireCertificateArgs, o: Option<&str>) -> Result<Certificate, WalletError> { self.0.acquire_certificate(a, o).await }
    async fn list_certificates(&self, a: ListCertificatesArgs, o: Option<&str>) -> Result<ListCertificatesResult, WalletError> { self.0.list_certificates(a, o).await }
    async fn prove_certificate(&self, a: ProveCertificateArgs, o: Option<&str>) -> Result<ProveCertificateResult, WalletError> { self.0.prove_certificate(a, o).await }
    async fn relinquish_certificate(&self, a: RelinquishCertificateArgs, o: Option<&str>) -> Result<RelinquishCertificateResult, WalletError> { self.0.relinquish_certificate(a, o).await }
    async fn discover_by_identity_key(&self, a: DiscoverByIdentityKeyArgs, o: Option<&str>) -> Result<DiscoverCertificatesResult, WalletError> { self.0.discover_by_identity_key(a, o).await }
    async fn discover_by_attributes(&self, a: DiscoverByAttributesArgs, o: Option<&str>) -> Result<DiscoverCertificatesResult, WalletError> { self.0.discover_by_attributes(a, o).await }
    async fn is_authenticated(&self, o: Option<&str>) -> Result<AuthenticatedResult, WalletError> { self.0.is_authenticated(o).await }
    async fn wait_for_authentication(&self, o: Option<&str>) -> Result<AuthenticatedResult, WalletError> { self.0.wait_for_authentication(o).await }
    async fn get_height(&self, o: Option<&str>) -> Result<GetHeightResult, WalletError> { self.0.get_height(o).await }
    async fn get_header_for_height(&self, a: GetHeaderArgs, o: Option<&str>) -> Result<GetHeaderResult, WalletError> { self.0.get_header_for_height(a, o).await }
    async fn get_network(&self, o: Option<&str>) -> Result<GetNetworkResult, WalletError> { self.0.get_network(o).await }
    async fn get_version(&self, o: Option<&str>) -> Result<GetVersionResult, WalletError> { self.0.get_version(o).await }
}
