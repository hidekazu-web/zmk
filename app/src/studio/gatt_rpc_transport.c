/*
 * Copyright (c) 2024 The ZMK Contributors
 *
 * SPDX-License-Identifier: MIT
 */

#include <zephyr/device.h>
#include <zephyr/init.h>
#include <sys/types.h>
#include <zephyr/sys/atomic.h>
#include <zephyr/kernel.h>
#include <zephyr/bluetooth/gatt.h>
#include <zephyr/sys/ring_buffer.h>

#include <zmk/ble.h>
#include <zmk/event_manager.h>
#include <zmk/events/ble_active_profile_changed.h>
#include <zmk/studio/rpc.h>
#include "uuid.h"

#include <zephyr/logging/log.h>

LOG_MODULE_DECLARE(zmk_studio, CONFIG_ZMK_STUDIO_LOG_LEVEL);

static bool handling_rx = false;

static K_SEM_DEFINE(indicate_sem, 1, 1);
static atomic_t notify_size;

static void rpc_ccc_cfg_changed(const struct bt_gatt_attr *attr, uint16_t value) {
    ARG_UNUSED(attr);

    bool notif_enabled = (value == BT_GATT_CCC_INDICATE);

    LOG_INF("RPC Notifications %s", notif_enabled ? "enabled" : "disabled");

#if CONFIG_ZMK_STUDIO_TRANSPORT_BLE_PREF_LATENCY < CONFIG_BT_PERIPHERAL_PREF_LATENCY
    struct bt_conn *conn = zmk_ble_active_profile_conn();
    if (conn) {
        uint8_t latency = notif_enabled ? CONFIG_ZMK_STUDIO_TRANSPORT_BLE_PREF_LATENCY
                                        : CONFIG_BT_PERIPHERAL_PREF_LATENCY;

        int ret = bt_conn_le_param_update(
            conn,
            BT_LE_CONN_PARAM(CONFIG_BT_PERIPHERAL_PREF_MIN_INT, CONFIG_BT_PERIPHERAL_PREF_MAX_INT,
                             latency, CONFIG_BT_PERIPHERAL_PREF_TIMEOUT));
        if (ret < 0) {
            LOG_WRN("Failed to request lower latency while studio is active (%d)", ret);
        }

        bt_conn_unref(conn);
    }
#endif
}

static ssize_t read_rpc_resp(struct bt_conn *conn, const struct bt_gatt_attr *attr, void *buf,
                             uint16_t len, uint16_t offset) {

    LOG_DBG("Read response for length %d at offset %d", len, offset);
    return 0;
}

static ssize_t write_rpc_req(struct bt_conn *conn, const struct bt_gatt_attr *attr, const void *buf,
                             uint16_t len, uint16_t offset, uint8_t flags) {
    if (!handling_rx) {
        return len;
    }

    uint32_t copied = 0;
    struct ring_buf *rpc_buf = zmk_rpc_get_rx_buf();

    /* This callback runs on the Bluetooth RX thread. If the RPC thread has stopped
     * draining the RX ring buffer, the copy loop below would spin forever and take
     * the whole Bluetooth stack down with it. Only the RPC thread removes data, so
     * free space can only grow after this check: once it passes, the loop ends. */
    if (ring_buf_space_get(rpc_buf) < len) {
        LOG_WRN("RPC RX buffer full, rejecting %d bytes", len);
        return BT_GATT_ERR(BT_ATT_ERR_INSUFFICIENT_RESOURCES);
    }

    while (copied < len) {
        uint8_t *buffer;
        uint32_t claim_len = ring_buf_put_claim(rpc_buf, &buffer, len - copied);

        if (claim_len > 0) {
            memcpy(buffer, ((uint8_t *)buf) + copied, claim_len);
            copied += claim_len;
        }

        ring_buf_put_finish(rpc_buf, claim_len);
    }

    zmk_rpc_rx_notify();

    return len;
}

BT_GATT_SERVICE_DEFINE(
    rpc_interface, BT_GATT_PRIMARY_SERVICE(BT_UUID_DECLARE_128(ZMK_STUDIO_BT_SERVICE_UUID)),
    BT_GATT_CHARACTERISTIC(BT_UUID_DECLARE_128(ZMK_STUDIO_BT_RPC_CHRC_UUID),
                           BT_GATT_CHRC_WRITE | BT_GATT_CHRC_READ | BT_GATT_CHRC_INDICATE,
                           BT_GATT_PERM_READ_ENCRYPT | BT_GATT_PERM_WRITE_ENCRYPT, read_rpc_resp,
                           write_rpc_req, NULL),
    BT_GATT_CCC(rpc_ccc_cfg_changed, BT_GATT_PERM_READ_ENCRYPT | BT_GATT_PERM_WRITE_ENCRYPT));

static uint16_t get_notify_size_for_conn(struct bt_conn *conn) {
    uint16_t notify_size = 23; // Default MTU size unless negotiated higher
    struct bt_conn_info conn_info;
    if (conn && bt_conn_get_info(conn, &conn_info) >= 0) {
        notify_size = conn_info.le.data_len->tx_max_len;
    }

    return notify_size;
}

static void refresh_notify_size(void) {
    struct bt_conn *conn = zmk_ble_active_profile_conn();

    uint16_t ns = get_notify_size_for_conn(conn);
    if (conn) {
        bt_conn_unref(conn);
    }

    atomic_set(&notify_size, ns);
}

static int gatt_start_rx() {
    refresh_notify_size();
    handling_rx = true;
    return 0;
}

static int gatt_stop_rx(void) {
    handling_rx = false;
    return 0;
}

static uint8_t indicate_buffer[27];

static void indicate_cb(struct bt_conn *conn, struct bt_gatt_indicate_params *params, uint8_t err);

static struct bt_gatt_indicate_params rpc_indicate_params = {
    .attr = &rpc_interface.attrs[1],
    .data = indicate_buffer,
    .func = indicate_cb,
};

/* How long a chunk the Bluetooth stack could not take right now is retried before the
 * queued response is dropped. */
#define INDICATE_RETRY_DELAY K_MSEC(20)
#define INDICATE_RETRY_LIMIT_MS 2000

/* A chunk already taken out of the TX ring buffer that the Bluetooth stack has not
 * accepted yet. Like the ring buffer's read side, only touched on the system work
 * queue thread (notif_rpc_tx_cb). */
static uint16_t indicate_held_len;
static int64_t indicate_held_since;

static void notify_tx_retry_cb(struct k_work *work);
static K_WORK_DELAYABLE_DEFINE(notify_tx_retry_work, notify_tx_retry_cb);

/* Throw away everything queued for sending. Reads from the consumer side instead of
 * ring_buf_reset() because the RPC thread may be in the middle of writing. */
static void discard_tx(struct ring_buf *tx_buf) {
    indicate_held_len = 0;
    ring_buf_get(tx_buf, NULL, ring_buf_size_get(tx_buf));
}

static void notif_rpc_tx_cb(struct k_work *work) {
    struct bt_conn *conn = zmk_ble_active_profile_conn();
    struct ring_buf *tx_buf = zmk_rpc_get_tx_buf();

    if (!conn) {
        LOG_WRN("No active connection for queued data, dropping");
        discard_tx(tx_buf);
        return;
    }

    if (indicate_held_len == 0 && ring_buf_size_get(tx_buf) == 0) {
        goto out;
    }

    /* An indication is still waiting for its confirmation; indicate_cb() resubmits. */
    if (k_sem_take(&indicate_sem, K_NO_WAIT) < 0) {
        goto out;
    }

    if (indicate_held_len == 0) {
        uint16_t notify_size = MIN(get_notify_size_for_conn(conn), sizeof(indicate_buffer));
        uint16_t added = 0;
        while (added < notify_size && ring_buf_size_get(tx_buf) > 0) {
            uint8_t *buf;
            int len = ring_buf_get_claim(tx_buf, &buf, notify_size - added);

            memcpy(indicate_buffer + added, buf, len);

            added += len;
            ring_buf_get_finish(tx_buf, len);
        }

        indicate_held_len = added;
        indicate_held_since = k_uptime_get();
    }

    rpc_indicate_params.len = indicate_held_len;

    int err = bt_gatt_indicate(conn, &rpc_indicate_params);
    if (err == 0) {
        /* indicate_cb() releases the semaphore and continues with the next chunk. */
        indicate_held_len = 0;
        goto out;
    }

    k_sem_give(&indicate_sem);

    /* Nothing calls us again after a failed indication unless we arrange it, and the
     * RPC thread waits forever for the TX ring buffer to drain. So either try this
     * chunk again shortly, or give the queued response up so the writer can go on. */
    bool transient = (err == -ENOMEM || err == -EAGAIN);
    if (transient && k_uptime_get() - indicate_held_since < INDICATE_RETRY_LIMIT_MS) {
        k_work_reschedule(&notify_tx_retry_work, INDICATE_RETRY_DELAY);
    } else {
        LOG_WRN("Failed to notify the response %d, dropping queued data", err);
        discard_tx(tx_buf);
    }

out:
    bt_conn_unref(conn);
}

static K_WORK_DEFINE(notify_tx_work, notif_rpc_tx_cb);

static void notify_tx_retry_cb(struct k_work *work) { k_work_submit(&notify_tx_work); }

struct gatt_write_state {
    size_t pending_notify;
};

static void indicate_cb(struct bt_conn *conn, struct bt_gatt_indicate_params *params, uint8_t err) {
    k_sem_give(&indicate_sem);
    k_work_submit(&notify_tx_work);
}

static void gatt_tx_notify(struct ring_buf *tx_buf, size_t added, bool msg_done, void *user_data) {
    struct gatt_write_state *state = (struct gatt_write_state *)user_data;

    state->pending_notify += added;

    atomic_t ns = atomic_get(&notify_size);

    /* rpc.c calls with added == 0 when the TX ring buffer is full and it is waiting
     * for room. That must always start a flush: pending_notify is usually below the
     * threshold at that point, and if the chain of indications has stopped nothing
     * else would ever drain the buffer. */
    bool waiting_for_room = (added == 0);

    if (msg_done || waiting_for_room || state->pending_notify > ns) {
        if (k_current_get() == k_work_queue_thread_get(&k_sys_work_q)) {
            /* We are already running on the system work queue (e.g. a Studio RPC
             * notification raised from a listener on that queue). Submitting
             * notify_tx_work here would never run: this thread is busy encoding
             * the response and blocks in send_response()'s k_sleep() loop
             * waiting for the TX ring buffer to drain, but the work item that
             * drains it can only run once this thread yields the queue. That is
             * a deadlock. Flush directly instead so the first bt_gatt_indicate()
             * is issued; indicate_cb() then resubmits on the system work queue to
             * drain the rest asynchronously. */
            notif_rpc_tx_cb(&notify_tx_work);
        } else {
            k_work_submit(&notify_tx_work);
        }

        state->pending_notify = 0;
    }
}

static struct gatt_write_state tx_state = {};

static void *gatt_tx_user_data(void) {
    memset(&tx_state, 0, sizeof(tx_state));

    return &tx_state;
}

ZMK_RPC_TRANSPORT(gatt, ZMK_TRANSPORT_BLE, gatt_start_rx, gatt_stop_rx, gatt_tx_user_data,
                  gatt_tx_notify);

static int gatt_rpc_listener(const zmk_event_t *eh) {
    refresh_notify_size();

#if IS_ENABLED(CONFIG_ZMK_STUDIO_LOCK_ON_DISCONNECT)
    struct bt_conn *conn = zmk_ble_active_profile_conn();

    if (!conn) {
        zmk_studio_core_lock();
    } else {
        bt_conn_unref(conn);
    }
#endif

    return 0;
}

ZMK_LISTENER(gatt_rpc_listener, gatt_rpc_listener);
ZMK_SUBSCRIPTION(gatt_rpc_listener, zmk_ble_active_profile_changed);
