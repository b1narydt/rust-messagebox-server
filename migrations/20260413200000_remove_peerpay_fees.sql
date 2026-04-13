-- Roll back PeerPay fees from the prior migration.
--
-- Background: setting delivery_fee > 0 on `payment_inbox` and `payment_requests`
-- broke PeerPay sends because the upstream @bsv/message-box-client library does
-- not pass `checkPermissions: true` from PeerPayClient.sendPayment / sendPaymentRequest.
-- Until that's patched upstream, paid PeerPay boxes return ERR_MISSING_PAYMENT_TX
-- on every send.
--
-- `chat` stays at 10 sats because clients hitting it can opt into checkPermissions
-- explicitly.
UPDATE server_fees SET delivery_fee = 0
WHERE message_box IN ('payment_inbox', 'payment_requests');
