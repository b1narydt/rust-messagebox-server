-- Phase 1A: tiered messageBox pricing for PeerPay traffic.
--
-- Charges 100 sats per delivery on the two PeerPay messageBoxes that initiate
-- value flow (incoming payments and outbound payment requests). Replies stay
-- free so we don't tax counterparties for declining a request.
--
-- Senders to these boxes must attach a payment that satisfies
-- (delivery_fee + recipient_fee). The delivery_fee credits the server
-- operator's wallet on storage.babbage.systems via the wallet-toolbox
-- internalize_action path wired in send_message.rs.
--
-- ON DUPLICATE KEY UPDATE is required for `payment_inbox` because the initial
-- schema seeds it at 0; INSERT IGNORE alone would leave it free. The two new
-- boxes are auto-created on first send so no separate INSERT INTO messageBox
-- is needed.
INSERT INTO server_fees (message_box, delivery_fee) VALUES
  ('payment_inbox', 100),
  ('payment_requests', 100),
  ('chat', 10)
ON DUPLICATE KEY UPDATE
  delivery_fee = VALUES(delivery_fee);
