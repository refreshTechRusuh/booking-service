-- migrations/001_initial_schema.sql
-- Booking Service: booking, booking_item, payment, payment_callback_log
-- event dan ticket_category di-own oleh event-service; di sini hanya foreign key reference.

-- booking
CREATE TABLE IF NOT EXISTS booking (
    id                  UUID PRIMARY KEY,
    user_id             UUID,                          -- NULL untuk guest
    event_id            UUID NOT NULL,
    email               TEXT NOT NULL,                 -- tujuan e-tiket
    status              TEXT NOT NULL DEFAULT 'HELD',  -- HELD|PAID|EXPIRED|CANCELLED
    booking_expires_at  TIMESTAMPTZ,                   -- dari Access Token TTL (5m)
    payment_expires_at  TIMESTAMPTZ,                   -- dari saat booking dibuat (10m)
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT booking_status_check CHECK (status IN ('HELD','PAID','EXPIRED','CANCELLED'))
);

-- booking_item: 1 baris per tiket, 1 tiket = 1 NIK penonton
CREATE TABLE IF NOT EXISTS booking_item (
    id              UUID PRIMARY KEY,
    booking_id      UUID NOT NULL REFERENCES booking(id) ON DELETE CASCADE,
    category_id     UUID NOT NULL,                     -- FK ke ticket_category (event-service)
    nik             CHAR(16) NOT NULL,                 -- NIK penonton, diverifikasi Dukcapil
    price_snapshot  BIGINT NOT NULL                    -- harga saat booking (immutable)
);

-- payment
CREATE TABLE IF NOT EXISTS payment (
    id              UUID PRIMARY KEY,
    booking_id      UUID NOT NULL REFERENCES booking(id),
    method          TEXT,                              -- bca_va|qris|gopay|...
    status          TEXT NOT NULL DEFAULT 'PENDING',   -- PENDING|SUCCESS|FAILED|EXPIRED
    provider_ref    TEXT,                              -- Midtrans order_id / transaction_id
    snap_token      TEXT,                              -- Midtrans Snap token
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT payment_status_check CHECK (status IN ('PENDING','SUCCESS','FAILED','EXPIRED'))
);

-- payment_callback_log: idempotency guard untuk webhook Midtrans
CREATE TABLE IF NOT EXISTS payment_callback_log (
    idempotency_key TEXT PRIMARY KEY,                  -- order_id + transaction_status
    booking_id      UUID,
    result          TEXT,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index untuk query kritis
CREATE INDEX IF NOT EXISTS idx_booking_event_status ON booking(event_id, status);
CREATE INDEX IF NOT EXISTS idx_booking_item_nik ON booking_item(nik);
CREATE INDEX IF NOT EXISTS idx_booking_payment_expires ON booking(payment_expires_at) WHERE status = 'HELD';
CREATE INDEX IF NOT EXISTS idx_payment_booking_id ON payment(booking_id);
