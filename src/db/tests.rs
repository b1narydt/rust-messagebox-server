//! MySQL-backed port of the original rusqlite tests. Each test gets its own
//! fresh per-test database via `crate::test_support::fresh_pool()`.

use super::queries::*;
use crate::test_support::{fresh_pool, TEST_KEY};

const TEST_KEY2: &str = "0350b59e3efb8e37ba1ba2bde37c24e2bed89346ef3dc46d780e2b99f3efe50d1c";

// ---------------------------------------------------------------------------
// Message-box helpers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ensure_message_box_creates_new() {
    let pool = fresh_pool().await;
    let id = ensure_message_box(&pool, TEST_KEY, "inbox").await.unwrap();
    assert!(id > 0, "messageBoxId should be non-zero");
}

#[tokio::test]
async fn test_ensure_message_box_idempotent() {
    let pool = fresh_pool().await;
    let id1 = ensure_message_box(&pool, TEST_KEY, "inbox").await.unwrap();
    let id2 = ensure_message_box(&pool, TEST_KEY, "inbox").await.unwrap();
    assert_eq!(id1, id2, "same params should return same id");
}

#[tokio::test]
async fn test_get_message_box_id_not_found() {
    let pool = fresh_pool().await;
    let result = get_message_box_id(&pool, TEST_KEY, "nonexistent").await.unwrap();
    assert!(result.is_none(), "should return None for non-existent box");
}

#[tokio::test]
async fn test_get_message_box_id_found() {
    let pool = fresh_pool().await;
    let id = ensure_message_box(&pool, TEST_KEY, "inbox").await.unwrap();
    let found = get_message_box_id(&pool, TEST_KEY, "inbox").await.unwrap();
    assert_eq!(found, Some(id));
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_message() {
    let pool = fresh_pool().await;
    let mb_id = ensure_message_box(&pool, TEST_KEY, "inbox").await.unwrap();
    let ok = insert_message(&pool, "msg-1", mb_id, TEST_KEY2, TEST_KEY, "hello").await.unwrap();
    assert!(ok, "insert should succeed");
}

#[tokio::test]
async fn test_insert_message_duplicate() {
    let pool = fresh_pool().await;
    let mb_id = ensure_message_box(&pool, TEST_KEY, "inbox").await.unwrap();
    insert_message(&pool, "msg-1", mb_id, TEST_KEY2, TEST_KEY, "hello").await.unwrap();
    let ok = insert_message(&pool, "msg-1", mb_id, TEST_KEY2, TEST_KEY, "hello again").await.unwrap();
    assert!(!ok, "duplicate messageId should return false");
}

#[tokio::test]
async fn test_list_messages() {
    let pool = fresh_pool().await;
    let mb_id = ensure_message_box(&pool, TEST_KEY, "inbox").await.unwrap();
    insert_message(&pool, "msg-1", mb_id, TEST_KEY2, TEST_KEY, "first").await.unwrap();
    insert_message(&pool, "msg-2", mb_id, TEST_KEY2, TEST_KEY, "second").await.unwrap();

    let msgs = list_messages(&pool, TEST_KEY, mb_id).await.unwrap();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].message_id, "msg-1");
    assert_eq!(msgs[0].body, "first");
    assert_eq!(msgs[1].message_id, "msg-2");
    assert_eq!(msgs[1].body, "second");
}

#[tokio::test]
async fn test_list_messages_empty() {
    let pool = fresh_pool().await;
    let mb_id = ensure_message_box(&pool, TEST_KEY, "inbox").await.unwrap();
    let msgs = list_messages(&pool, TEST_KEY, mb_id).await.unwrap();
    assert!(msgs.is_empty());
}

#[tokio::test]
async fn test_acknowledge_messages() {
    let pool = fresh_pool().await;
    let mb_id = ensure_message_box(&pool, TEST_KEY, "inbox").await.unwrap();
    insert_message(&pool, "msg-1", mb_id, TEST_KEY2, TEST_KEY, "first").await.unwrap();
    insert_message(&pool, "msg-2", mb_id, TEST_KEY2, TEST_KEY, "second").await.unwrap();
    insert_message(&pool, "msg-3", mb_id, TEST_KEY2, TEST_KEY, "third").await.unwrap();

    let ids = vec!["msg-1".to_string(), "msg-2".to_string()];
    let deleted = acknowledge_messages(&pool, TEST_KEY, &ids).await.unwrap();
    assert_eq!(deleted, 2);

    let remaining = list_messages(&pool, TEST_KEY, mb_id).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].message_id, "msg-3");
}

#[tokio::test]
async fn test_acknowledge_messages_not_found() {
    let pool = fresh_pool().await;
    let ids = vec!["nonexistent-1".to_string()];
    let deleted = acknowledge_messages(&pool, TEST_KEY, &ids).await.unwrap();
    assert_eq!(deleted, 0);
}

// ---------------------------------------------------------------------------
// Fees
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_get_server_delivery_fee_default() {
    let pool = fresh_pool().await;
    let fee = get_server_delivery_fee(&pool, "notifications").await.unwrap();
    assert_eq!(fee, 10, "notifications box default delivery fee should be 10");

    let fee = get_server_delivery_fee(&pool, "inbox").await.unwrap();
    assert_eq!(fee, 0, "inbox box default delivery fee should be 0");
}

#[tokio::test]
async fn test_get_server_delivery_fee_unknown() {
    let pool = fresh_pool().await;
    let fee = get_server_delivery_fee(&pool, "unknown_box").await.unwrap();
    assert_eq!(fee, 0, "unknown box should return 0");
}

// ---------------------------------------------------------------------------
// Recipient fees (hierarchical lookup)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_get_recipient_fee_sender_specific() {
    let pool = fresh_pool().await;
    // Set a sender-specific fee
    set_message_permission(&pool, TEST_KEY, Some(TEST_KEY2), "inbox", 42).await.unwrap();

    let fee = get_recipient_fee(&pool, TEST_KEY, TEST_KEY2, "inbox").await.unwrap();
    assert_eq!(fee, 42, "should return sender-specific fee");
}

#[tokio::test]
async fn test_get_recipient_fee_box_wide_fallback() {
    let pool = fresh_pool().await;
    // Set a box-wide fee (sender = NULL)
    set_message_permission(&pool, TEST_KEY, None, "inbox", 25).await.unwrap();

    // Query with a sender that has no specific permission
    let fee = get_recipient_fee(&pool, TEST_KEY, TEST_KEY2, "inbox").await.unwrap();
    assert_eq!(fee, 25, "should fall back to box-wide fee");
}

#[tokio::test]
async fn test_get_recipient_fee_auto_create_default() {
    let pool = fresh_pool().await;

    // No permissions set at all for this recipient/box - should auto-create
    let fee = get_recipient_fee(&pool, TEST_KEY, TEST_KEY2, "notifications").await.unwrap();
    assert_eq!(fee, 10, "notifications box smart default should be 10");

    let fee = get_recipient_fee(&pool, TEST_KEY, TEST_KEY2, "inbox").await.unwrap();
    assert_eq!(fee, 0, "inbox box smart default should be 0");
}

// ---------------------------------------------------------------------------
// Permissions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_set_message_permission() {
    let pool = fresh_pool().await;
    let ok = set_message_permission(&pool, TEST_KEY, Some(TEST_KEY2), "inbox", 50).await.unwrap();
    assert!(ok);

    let perm = get_permission(&pool, TEST_KEY, Some(TEST_KEY2), "inbox").await.unwrap();
    assert!(perm.is_some());
    let perm = perm.unwrap();
    assert_eq!(perm.recipient, TEST_KEY);
    assert_eq!(perm.sender.as_deref(), Some(TEST_KEY2));
    assert_eq!(perm.message_box, "inbox");
    assert_eq!(perm.recipient_fee, 50);
}

#[tokio::test]
async fn test_set_message_permission_null_sender() {
    let pool = fresh_pool().await;
    let ok = set_message_permission(&pool, TEST_KEY, None, "inbox", 30).await.unwrap();
    assert!(ok);

    let perm = get_permission(&pool, TEST_KEY, None, "inbox").await.unwrap();
    assert!(perm.is_some());
    let perm = perm.unwrap();
    assert_eq!(perm.recipient, TEST_KEY);
    assert!(perm.sender.is_none());
    assert_eq!(perm.recipient_fee, 30);
}

#[tokio::test]
async fn test_set_message_permission_update() {
    let pool = fresh_pool().await;
    set_message_permission(&pool, TEST_KEY, Some(TEST_KEY2), "inbox", 50).await.unwrap();
    set_message_permission(&pool, TEST_KEY, Some(TEST_KEY2), "inbox", 100).await.unwrap();

    let perm = get_permission(&pool, TEST_KEY, Some(TEST_KEY2), "inbox").await.unwrap().unwrap();
    assert_eq!(perm.recipient_fee, 100, "fee should be updated to 100");
}

#[tokio::test]
async fn test_get_permission_not_found() {
    let pool = fresh_pool().await;
    let perm = get_permission(&pool, TEST_KEY, Some(TEST_KEY2), "inbox").await.unwrap();
    assert!(perm.is_none());
}

#[tokio::test]
async fn test_list_permissions_with_pagination() {
    let pool = fresh_pool().await;
    // Insert several permissions
    set_message_permission(&pool, TEST_KEY, None, "inbox", 10).await.unwrap();
    set_message_permission(&pool, TEST_KEY, Some(TEST_KEY2), "inbox", 20).await.unwrap();
    set_message_permission(&pool, TEST_KEY, None, "notifications", 30).await.unwrap();

    // List all with limit
    let (perms, total) = list_permissions(&pool, TEST_KEY, None, 10, 0, "asc").await.unwrap();
    assert_eq!(total, 3);
    assert_eq!(perms.len(), 3);

    // List with pagination
    let (perms, total) = list_permissions(&pool, TEST_KEY, None, 2, 0, "asc").await.unwrap();
    assert_eq!(total, 3);
    assert_eq!(perms.len(), 2);

    let (perms, total) = list_permissions(&pool, TEST_KEY, None, 2, 2, "asc").await.unwrap();
    assert_eq!(total, 3);
    assert_eq!(perms.len(), 1);

    // Filter by messageBox
    let (perms, total) = list_permissions(&pool, TEST_KEY, Some("inbox"), 10, 0, "asc").await.unwrap();
    assert_eq!(total, 2);
    assert_eq!(perms.len(), 2);
}

// ---------------------------------------------------------------------------
// Devices
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_register_device() {
    let pool = fresh_pool().await;
    let id = register_device(&pool, TEST_KEY, "fcm-token-abc", Some("dev-1"), Some("android")).await.unwrap();
    assert!(id > 0);
}

#[tokio::test]
async fn test_register_device_upsert() {
    let pool = fresh_pool().await;
    register_device(&pool, TEST_KEY, "fcm-token-abc", Some("dev-1"), Some("android")).await.unwrap();
    // Same fcm_token, different identity_key should upsert
    register_device(&pool, TEST_KEY2, "fcm-token-abc", Some("dev-2"), Some("ios")).await.unwrap();

    let devices = list_devices(&pool, TEST_KEY2).await.unwrap();
    assert_eq!(devices.len(), 1, "upsert should update, not duplicate");
    assert_eq!(devices[0].platform.as_deref(), Some("ios"));
    assert_eq!(devices[0].device_id.as_deref(), Some("dev-2"));
}

#[tokio::test]
async fn test_list_devices() {
    let pool = fresh_pool().await;
    register_device(&pool, TEST_KEY, "token-1", None, Some("android")).await.unwrap();
    register_device(&pool, TEST_KEY, "token-2", None, Some("ios")).await.unwrap();

    let devices = list_devices(&pool, TEST_KEY).await.unwrap();
    assert_eq!(devices.len(), 2);
}

#[tokio::test]
async fn test_list_active_devices() {
    let pool = fresh_pool().await;
    let id1 = register_device(&pool, TEST_KEY, "token-1", None, Some("android")).await.unwrap();
    register_device(&pool, TEST_KEY, "token-2", None, Some("ios")).await.unwrap();

    // Deactivate the first one
    deactivate_device(&pool, id1).await.unwrap();

    let active = list_active_devices(&pool, TEST_KEY).await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].fcm_token, "token-2");
}

#[tokio::test]
async fn test_deactivate_device() {
    let pool = fresh_pool().await;
    let id = register_device(&pool, TEST_KEY, "token-1", None, None).await.unwrap();
    deactivate_device(&pool, id).await.unwrap();

    let devices = list_devices(&pool, TEST_KEY).await.unwrap();
    assert_eq!(devices.len(), 1);
    assert!(!devices[0].active, "device should be inactive after deactivation");
}

// ---------------------------------------------------------------------------
// FCM delivery check
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_should_use_fcm_delivery() {
    assert!(should_use_fcm_delivery("notifications"));
    assert!(!should_use_fcm_delivery("inbox"));
    assert!(!should_use_fcm_delivery("payment_inbox"));
    assert!(!should_use_fcm_delivery("random"));
}

// ---------------------------------------------------------------------------
// Delivery fee cache
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_delivery_fee_cache() {
    let pool = fresh_pool().await;
    // After init_delivery_fee_cache (called in fresh_pool), fees should come from cache
    let fee = get_server_delivery_fee(&pool, "notifications").await.unwrap();
    assert_eq!(fee, 10, "notifications delivery fee should be 10");

    let fee = get_server_delivery_fee(&pool, "inbox").await.unwrap();
    assert_eq!(fee, 0, "inbox delivery fee should be 0");

    let fee = get_server_delivery_fee(&pool, "unknown").await.unwrap();
    assert_eq!(fee, 0, "unknown box delivery fee should be 0");
}

// ---------------------------------------------------------------------------
// Consolidated permission lookup
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_consolidated_permission_lookup() {
    let pool = fresh_pool().await;
    // Set box-wide fee=10
    set_message_permission(&pool, TEST_KEY, None, "inbox", 10).await.unwrap();
    // Set sender-specific fee=50
    set_message_permission(&pool, TEST_KEY, Some(TEST_KEY2), "inbox", 50).await.unwrap();

    // Sender-specific should win over box-wide
    let fee = get_recipient_fee(&pool, TEST_KEY, TEST_KEY2, "inbox").await.unwrap();
    assert_eq!(fee, 50, "sender-specific fee should win over box-wide");
}

// ---------------------------------------------------------------------------
// Cross-user message isolation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_cross_user_message_isolation() {
    let pool = fresh_pool().await;

    // Create inbox for user A and send a message to user A
    let mb_id_a = ensure_message_box(&pool, TEST_KEY, "inbox").await.unwrap();
    insert_message(&pool, "msg-isolated-1", mb_id_a, TEST_KEY2, TEST_KEY, "for user A").await.unwrap();

    // Create inbox for user B
    let mb_id_b = ensure_message_box(&pool, TEST_KEY2, "inbox").await.unwrap();

    // List messages as user B - should be empty
    let msgs = list_messages(&pool, TEST_KEY2, mb_id_b).await.unwrap();
    assert!(msgs.is_empty(), "user B should see no messages in their own box");

    // Also listing from user A's box as user B should yield nothing (recipient mismatch)
    let msgs = list_messages(&pool, TEST_KEY2, mb_id_a).await.unwrap();
    assert!(msgs.is_empty(), "user B should not see user A's messages");

    // Acknowledge as user B should delete nothing
    let ids = vec!["msg-isolated-1".to_string()];
    let deleted = acknowledge_messages(&pool, TEST_KEY2, &ids).await.unwrap();
    assert_eq!(deleted, 0, "user B should not be able to acknowledge user A's messages");

    // Verify message still exists for user A
    let msgs = list_messages(&pool, TEST_KEY, mb_id_a).await.unwrap();
    assert_eq!(msgs.len(), 1, "user A's message should still be there");
}

// ---------------------------------------------------------------------------
// Acknowledge multiple messages
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_acknowledge_multiple_messages() {
    let pool = fresh_pool().await;
    let mb_id = ensure_message_box(&pool, TEST_KEY, "inbox").await.unwrap();

    // Insert 5 messages
    for i in 1..=5 {
        insert_message(
            &pool,
            &format!("msg-multi-{}", i),
            mb_id,
            TEST_KEY2,
            TEST_KEY,
            &format!("body {}", i),
        )
        .await
        .unwrap();
    }

    // Acknowledge 3 of them
    let ids: Vec<String> = (1..=3).map(|i| format!("msg-multi-{}", i)).collect();
    let deleted = acknowledge_messages(&pool, TEST_KEY, &ids).await.unwrap();
    assert_eq!(deleted, 3, "should delete exactly 3 messages");

    // Verify 2 remain
    let remaining = list_messages(&pool, TEST_KEY, mb_id).await.unwrap();
    assert_eq!(remaining.len(), 2, "should have 2 remaining messages");
    assert_eq!(remaining[0].message_id, "msg-multi-4");
    assert_eq!(remaining[1].message_id, "msg-multi-5");
}

// ---------------------------------------------------------------------------
// Message ordering
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_message_ordering() {
    let pool = fresh_pool().await;
    let mb_id = ensure_message_box(&pool, TEST_KEY, "inbox").await.unwrap();

    // Insert 5 messages
    for i in 1..=5 {
        insert_message(
            &pool,
            &format!("msg-order-{}", i),
            mb_id,
            TEST_KEY2,
            TEST_KEY,
            &format!("body {}", i),
        )
        .await
        .unwrap();
    }

    // List them - verify created_at ASC order (insertion order)
    let msgs = list_messages(&pool, TEST_KEY, mb_id).await.unwrap();
    assert_eq!(msgs.len(), 5);
    for (i, msg) in msgs.iter().enumerate() {
        assert_eq!(msg.message_id, format!("msg-order-{}", i + 1));
    }
    // Verify ascending order by created_at
    for i in 1..msgs.len() {
        assert!(
            msgs[i].created_at >= msgs[i - 1].created_at,
            "messages should be ordered by created_at ASC"
        );
    }
}

// ---------------------------------------------------------------------------
// Permission blocking
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_permission_blocking() {
    let pool = fresh_pool().await;
    // Set fee=-1 for a recipient (blocking)
    set_message_permission(&pool, TEST_KEY, Some(TEST_KEY2), "inbox", -1).await.unwrap();

    let fee = get_recipient_fee(&pool, TEST_KEY, TEST_KEY2, "inbox").await.unwrap();
    assert_eq!(fee, -1, "fee should be -1 indicating blocked sender");
}

// ---------------------------------------------------------------------------
// Concurrent access (tokio tasks against MySQL pool)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_concurrent_message_operations() {
    let pool = fresh_pool().await;

    // Pre-create a message box
    let mb_id = ensure_message_box(&pool, TEST_KEY, "inbox").await.unwrap();

    let mut tasks = tokio::task::JoinSet::new();
    for thread_idx in 0..4u32 {
        let pool = pool.clone();
        tasks.spawn(async move {
            for i in 0..10u32 {
                let msg_id = format!("concurrent-{thread_idx}-{i}");
                insert_message(&pool, &msg_id, mb_id, TEST_KEY2, TEST_KEY, "body")
                    .await
                    .expect("insert should succeed");
            }
            let msgs = list_messages(&pool, TEST_KEY, mb_id).await.expect("list should succeed");
            assert!(!msgs.is_empty(), "should have messages after inserts");
        });
    }

    while let Some(res) = tasks.join_next().await {
        res.expect("task should not panic");
    }

    // Verify all 40 messages were inserted
    let all = list_messages(&pool, TEST_KEY, mb_id).await.unwrap();
    assert_eq!(all.len(), 40, "all 40 concurrent inserts should succeed");

    // Acknowledge some messages concurrently
    let mut ack_tasks = tokio::task::JoinSet::new();
    for thread_idx in 0..4u32 {
        let pool = pool.clone();
        ack_tasks.spawn(async move {
            let ids: Vec<String> = (0..5u32)
                .map(|i| format!("concurrent-{thread_idx}-{i}"))
                .collect();
            let deleted = acknowledge_messages(&pool, TEST_KEY, &ids)
                .await
                .expect("acknowledge should succeed");
            assert_eq!(deleted, 5, "should delete 5 messages per task");
        });
    }

    while let Some(res) = ack_tasks.join_next().await {
        res.expect("ack task should not panic");
    }

    let remaining = list_messages(&pool, TEST_KEY, mb_id).await.unwrap();
    assert_eq!(remaining.len(), 20, "20 messages should remain after acknowledging 20");
}
