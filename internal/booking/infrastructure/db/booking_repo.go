// Package db mengimplementasikan outbound.IBookingRepository dan IStockRepository
// menggunakan PostgreSQL via pgx/v5.
package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/dto"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/entity"
	"github.com/refreshTechRusuh/booking-service/internal/booking/port/outbound"
	"github.com/refreshTechRusuh/booking-service/pkg/apperror"
)

// asTx mengkonversi outbound.Tx ke pgx.Tx (infra layer, aman).
func asTx(tx outbound.Tx) pgx.Tx { return tx.(pgx.Tx) }

// sqlVerb menentukan jenis query dari KATA PERTAMA SQL asli (select/insert/update/delete/begin/commit/…).
// Akurat karena membaca SQL yang benar-benar dieksekusi, bukan menebak dari nama op.
func sqlVerb(sql string) string {
	s := strings.ToLower(strings.TrimSpace(sql))
	if i := strings.IndexAny(s, " \t\n"); i >= 0 {
		return s[:i]
	}
	if s == "" {
		return "query"
	}
	return s
}

// dbLog adalah helper untuk log query DB secara konsisten.
//   - INFO : ringkas — verb SQL (select/insert/update/delete) + op + duration + args.
//   - DEBUG: full SQL template (TANPA nilai args, jadi aman dari bocor PII seperti nik/email).
//   - ERROR: verb + full SQL + error, agar langsung terlihat tanpa menurunkan level.
//
// sql = query template dengan placeholder $1,$2 (bukan nilainya); untuk langkah
// transaksi kirim literal deskriptif seperti "BEGIN"/"COMMIT".
func dbLog(ctx context.Context, log *slog.Logger, op, sql string, start time.Time, err error, args ...any) {
	elapsed := time.Since(start)
	verb := sqlVerb(sql)
	base := append([]any{slog.String("op", op), slog.String("duration", elapsed.String())}, args...)
	if err != nil {
		log.ErrorContext(ctx, verb+" failed",
			append(base, slog.String("sql", sql), slog.String("error", err.Error()))...)
		return
	}
	log.InfoContext(ctx, verb, base...)
	// Full SQL hanya di DEBUG agar senyap di produksi (LOG_LEVEL=info).
	log.DebugContext(ctx, verb+" sql", append(base, slog.String("sql", sql))...)
}

// ---- BookingRepository ------------------------------------------------------

type BookingRepository struct {
	pool IPool
	log  *slog.Logger
}

func NewBookingRepository(pool IPool, log *slog.Logger) *BookingRepository {
	return &BookingRepository{pool: pool, log: log}
}

// CreateBookingTx menjalankan fn dalam satu transaksi pgx.
// Begin, defer Rollback, dan Commit dikelola di sini.
func (r *BookingRepository) CreateBookingTx(
	ctx context.Context,
	req dto.CreateBookingRequest,
	fn func(context.Context, outbound.Tx) (dto.CreateBookingResponse, error),
) (dto.CreateBookingResponse, error) {
	start := time.Now()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		dbLog(ctx, r.log, "booking.begin_tx", "BEGIN", start, err)
		return dto.CreateBookingResponse{}, apperror.Internal(fmt.Errorf("begin tx: %w", err))
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	result, err := fn(ctx, tx)
	if err != nil {
		return dto.CreateBookingResponse{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		dbLog(ctx, r.log, "booking.commit_tx", "COMMIT", start, err)
		return dto.CreateBookingResponse{}, apperror.Internal(fmt.Errorf("commit tx: %w", err))
	}
	dbLog(ctx, r.log, "booking.create_tx", "TX create booking", start, nil, slog.String("eventId", req.EventID))
	return result, nil
}

// Create menyimpan booking + booking_items dalam transaksi yang sudah berjalan.
func (r *BookingRepository) Create(ctx context.Context, tx outbound.Tx, b entity.Booking, items []entity.BookingItem) error {
	start := time.Now()
	ptx := asTx(tx)
	const qInsertBooking = `
		INSERT INTO booking (id, user_id, event_id, email, status, booking_expires_at, payment_expires_at, created_at)
		VALUES ($1, NULLIF($2,'')::uuid, $3, $4, $5, $6, $7, $8)`
	const qInsertItem = `
			INSERT INTO booking_item (id, booking_id, category_id, nik, price_snapshot)
			VALUES ($1, $2, $3, $4, $5)`
	_, err := ptx.Exec(ctx, qInsertBooking,
		b.ID, b.UserID, b.EventID, b.Email, b.Status, b.BookingExpiresAt, b.PaymentExpiresAt, b.CreatedAt)
	if err != nil {
		dbLog(ctx, r.log, "booking.insert", qInsertBooking, start, err, slog.String("bookingId", b.ID))
		return fmt.Errorf("booking create: %w", err)
	}
	for _, item := range items {
		if _, err := ptx.Exec(ctx, qInsertItem,
			item.ID, item.BookingID, item.CategoryID, item.NIK, item.PriceSnapshot); err != nil {
			dbLog(ctx, r.log, "booking_item.insert", qInsertItem, start, err, slog.String("bookingId", b.ID))
			return fmt.Errorf("booking_item create: %w", err)
		}
	}
	dbLog(ctx, r.log, "booking.insert", qInsertBooking, start, nil,
		slog.String("bookingId", b.ID),
		slog.String("eventId", b.EventID),
		slog.Int("items", len(items)))
	return nil
}

// GetByID mengembalikan booking beserta items-nya.
func (r *BookingRepository) GetByID(ctx context.Context, bookingID string) (entity.Booking, []entity.BookingItem, error) {
	start := time.Now()
	const qGetBooking = `
		SELECT id, COALESCE(user_id::text,''), event_id, email, status, booking_expires_at, payment_expires_at, created_at
		FROM booking WHERE id = $1`
	const qListItems = `
		SELECT id, booking_id, category_id, nik, price_snapshot FROM booking_item WHERE booking_id = $1`
	var b entity.Booking
	err := r.pool.QueryRow(ctx, qGetBooking, bookingID).
		Scan(&b.ID, &b.UserID, &b.EventID, &b.Email, &b.Status, &b.BookingExpiresAt, &b.PaymentExpiresAt, &b.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			dbLog(ctx, r.log, "booking.get_by_id", qGetBooking, start, err, slog.String("bookingId", bookingID))
			return entity.Booking{}, nil, apperror.NotFound("booking")
		}
		dbLog(ctx, r.log, "booking.get_by_id", qGetBooking, start, err, slog.String("bookingId", bookingID))
		return entity.Booking{}, nil, fmt.Errorf("booking get: %w", err)
	}
	rows, err := r.pool.Query(ctx, qListItems, bookingID)
	if err != nil {
		dbLog(ctx, r.log, "booking_item.list", qListItems, start, err, slog.String("bookingId", bookingID))
		return b, nil, err
	}
	defer rows.Close()
	var items []entity.BookingItem
	for rows.Next() {
		var it entity.BookingItem
		if err := rows.Scan(&it.ID, &it.BookingID, &it.CategoryID, &it.NIK, &it.PriceSnapshot); err != nil {
			return b, nil, err
		}
		items = append(items, it)
	}
	dbLog(ctx, r.log, "booking.get_by_id", qGetBooking, start, nil,
		slog.String("bookingId", bookingID),
		slog.Int("items", len(items)))
	return b, items, nil
}

// UpdateStatus memperbarui status booking dalam transaksi.
func (r *BookingRepository) UpdateStatus(ctx context.Context, tx outbound.Tx, bookingID string, status entity.BookingStatus) error {
	start := time.Now()
	const qUpdateStatus = `UPDATE booking SET status = $1 WHERE id = $2`
	_, err := asTx(tx).Exec(ctx, qUpdateStatus, status, bookingID)
	dbLog(ctx, r.log, "booking.update_status", qUpdateStatus, start, err,
		slog.String("bookingId", bookingID),
		slog.String("status", string(status)))
	return err
}

// ListExpiredHeld mengembalikan booking HELD yang payment_expires_at sudah lewat.
func (r *BookingRepository) ListExpiredHeld(ctx context.Context, limit int) ([]entity.Booking, error) {
	start := time.Now()
	const qListExpiredHeld = `
		SELECT id, COALESCE(user_id::text,''), event_id, email, status, booking_expires_at, payment_expires_at, created_at
		FROM booking WHERE status = 'HELD' AND payment_expires_at < NOW()
		LIMIT $1 FOR UPDATE SKIP LOCKED`
	rows, err := r.pool.Query(ctx, qListExpiredHeld, limit)
	if err != nil {
		dbLog(ctx, r.log, "booking.list_expired_held", qListExpiredHeld, start, err)
		return nil, err
	}
	defer rows.Close()
	var result []entity.Booking
	for rows.Next() {
		var b entity.Booking
		if err := rows.Scan(&b.ID, &b.UserID, &b.EventID, &b.Email, &b.Status, &b.BookingExpiresAt, &b.PaymentExpiresAt, &b.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, b)
	}
	dbLog(ctx, r.log, "booking.list_expired_held", qListExpiredHeld, start, nil, slog.Int("count", len(result)))
	return result, nil
}

// HasActiveHeld mengembalikan true bila masih ada booking HELD aktif untuk event ini.
func (r *BookingRepository) HasActiveHeld(ctx context.Context, eventID string) (bool, error) {
	start := time.Now()
	const qHasActiveHeld = `
		SELECT COUNT(*) FROM booking
		WHERE event_id = $1 AND status = 'HELD' AND payment_expires_at > NOW()
		LIMIT 1`
	var count int
	err := r.pool.QueryRow(ctx, qHasActiveHeld, eventID).Scan(&count)
	dbLog(ctx, r.log, "booking.has_active_held", qHasActiveHeld, start, err,
		slog.String("eventId", eventID),
		slog.Bool("result", count > 0))
	if err != nil {
		return false, fmt.Errorf("has active held: %w", err)
	}
	return count > 0, nil
}

// ExpireBookingTx mengeksekusi UPDATE booking=EXPIRED + ReleaseStock dalam satu transaksi.
func (r *BookingRepository) ExpireBookingTx(ctx context.Context, bookingID string) ([]entity.BookingItem, error) {
	start := time.Now()
	const qExpireBooking = `UPDATE booking SET status='EXPIRED' WHERE id=$1 AND status='HELD'`
	const qReleaseStock = `
		UPDATE ticket_category tc
		   SET available = available + sub.qty
		FROM (
			SELECT category_id, COUNT(*) AS qty
			FROM booking_item WHERE booking_id = $1
			GROUP BY category_id
		) sub
		WHERE tc.id = sub.category_id`
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		dbLog(ctx, r.log, "booking.expire_tx.begin", "BEGIN", start, err, slog.String("bookingId", bookingID))
		return nil, fmt.Errorf("expire booking tx begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	tag, err := tx.Exec(ctx, qExpireBooking, bookingID)
	if err != nil {
		dbLog(ctx, r.log, "booking.expire_tx.update_booking", qExpireBooking, start, err, slog.String("bookingId", bookingID))
		return nil, fmt.Errorf("expire booking update: %w", err)
	}
	// Booking sudah tidak HELD (keburu PAID/CANCELLED): jangan lepas stok (cegah double-release/oversell).
	if tag.RowsAffected() == 0 {
		return nil, outbound.ErrBookingNotHeld
	}

	if _, err := tx.Exec(ctx, qReleaseStock, bookingID); err != nil {
		dbLog(ctx, r.log, "booking.expire_tx.release_stock", qReleaseStock, start, err, slog.String("bookingId", bookingID))
		return nil, fmt.Errorf("expire booking release stock: %w", err)
	}

	rows, err := tx.Query(ctx,
		`SELECT id, booking_id, category_id, nik, price_snapshot FROM booking_item WHERE booking_id=$1`, bookingID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []entity.BookingItem
	for rows.Next() {
		var it entity.BookingItem
		if err := rows.Scan(&it.ID, &it.BookingID, &it.CategoryID, &it.NIK, &it.PriceSnapshot); err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	rows.Close()

	if err := tx.Commit(ctx); err != nil {
		dbLog(ctx, r.log, "booking.expire_tx.commit", "COMMIT", start, err, slog.String("bookingId", bookingID))
		return nil, fmt.Errorf("expire booking commit: %w", err)
	}
	dbLog(ctx, r.log, "booking.expire_tx", "TX expire booking", start, nil,
		slog.String("bookingId", bookingID),
		slog.Int("itemsReleased", len(items)))
	return items, nil
}

// CancelBookingTx membatalkan booking HELD secara atomik.
//
// Baris booking dikunci (FOR UPDATE) sehingga callback pembayaran / sweeper tidak bisa
// mengubah status bersamaan. Stok hanya dilepas bila UPDATE benar-benar mengenai 1 baris
// (HELD → CANCELLED), jadi tidak ada double-release.
func (r *BookingRepository) CancelBookingTx(ctx context.Context, bookingID string) (entity.CancelResult, error) {
	start := time.Now()
	const qLock = `SELECT event_id, status FROM booking WHERE id=$1 FOR UPDATE`
	const qPayment = `SELECT EXISTS(SELECT 1 FROM payment WHERE booking_id=$1 AND status IN ('PENDING','SUCCESS'))`
	const qCancel = `UPDATE booking SET status='CANCELLED' WHERE id=$1 AND status='HELD'`
	const qReleaseStock = `
		UPDATE ticket_category tc
		   SET available = available + sub.qty
		FROM (
			SELECT category_id, COUNT(*) AS qty
			FROM booking_item WHERE booking_id = $1
			GROUP BY category_id
		) sub
		WHERE tc.id = sub.category_id`
	const qItems = `SELECT id, booking_id, category_id, nik, price_snapshot FROM booking_item WHERE booking_id=$1`

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		dbLog(ctx, r.log, "booking.cancel_tx.begin", "BEGIN", start, err, slog.String("bookingId", bookingID))
		return entity.CancelResult{}, fmt.Errorf("cancel booking tx begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var eventID, status string
	if err := tx.QueryRow(ctx, qLock, bookingID).Scan(&eventID, &status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return entity.CancelResult{}, apperror.NotFound("booking")
		}
		dbLog(ctx, r.log, "booking.cancel_tx.lock", qLock, start, err, slog.String("bookingId", bookingID))
		return entity.CancelResult{}, fmt.Errorf("cancel booking lock: %w", err)
	}

	switch entity.BookingStatus(status) {
	case entity.BookingStatusCancelled:
		return entity.CancelResult{EventID: eventID, AlreadyCancelled: true}, nil
	case entity.BookingStatusPaid:
		return entity.CancelResult{}, apperror.BookingPaid()
	case entity.BookingStatusExpired:
		return entity.CancelResult{}, apperror.BookingExpired()
	}

	var hasPayment bool
	if err := tx.QueryRow(ctx, qPayment, bookingID).Scan(&hasPayment); err != nil {
		dbLog(ctx, r.log, "booking.cancel_tx.check_payment", qPayment, start, err, slog.String("bookingId", bookingID))
		return entity.CancelResult{}, fmt.Errorf("cancel booking check payment: %w", err)
	}
	if hasPayment {
		return entity.CancelResult{}, apperror.PaymentInProgress()
	}

	tag, err := tx.Exec(ctx, qCancel, bookingID)
	if err != nil {
		dbLog(ctx, r.log, "booking.cancel_tx.update", qCancel, start, err, slog.String("bookingId", bookingID))
		return entity.CancelResult{}, fmt.Errorf("cancel booking update: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return entity.CancelResult{}, fmt.Errorf("cancel booking: expected 1 row, got %d", tag.RowsAffected())
	}

	if _, err := tx.Exec(ctx, qReleaseStock, bookingID); err != nil {
		dbLog(ctx, r.log, "booking.cancel_tx.release_stock", qReleaseStock, start, err, slog.String("bookingId", bookingID))
		return entity.CancelResult{}, fmt.Errorf("cancel booking release stock: %w", err)
	}

	rows, err := tx.Query(ctx, qItems, bookingID)
	if err != nil {
		return entity.CancelResult{}, fmt.Errorf("cancel booking list items: %w", err)
	}
	defer rows.Close()
	var items []entity.BookingItem
	for rows.Next() {
		var it entity.BookingItem
		if err := rows.Scan(&it.ID, &it.BookingID, &it.CategoryID, &it.NIK, &it.PriceSnapshot); err != nil {
			return entity.CancelResult{}, err
		}
		items = append(items, it)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return entity.CancelResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		dbLog(ctx, r.log, "booking.cancel_tx.commit", "COMMIT", start, err, slog.String("bookingId", bookingID))
		return entity.CancelResult{}, fmt.Errorf("cancel booking commit: %w", err)
	}
	dbLog(ctx, r.log, "booking.cancel_tx", "TX cancel booking", start, nil,
		slog.String("bookingId", bookingID), slog.Int("itemsReleased", len(items)))
	return entity.CancelResult{EventID: eventID, Items: items}, nil
}

type StockRepository struct {
	pool IPool
	log  *slog.Logger
}

func NewStockRepository(pool IPool, log *slog.Logger) *StockRepository {
	return &StockRepository{pool: pool, log: log}
}

// HoldStock mengurangi available atomic — conditional UPDATE WHERE available >= qty.
func (r *StockRepository) HoldStock(ctx context.Context, tx outbound.Tx, items []entity.HoldItem) error {
	ptx := asTx(tx)
	const qHold = `UPDATE ticket_category SET available = available - $1 WHERE id = $2 AND available >= $1`
	for _, item := range items {
		start := time.Now()
		tag, err := ptx.Exec(ctx, qHold, item.Qty, item.CategoryID)
		if err != nil {
			dbLog(ctx, r.log, "stock.hold", qHold, start, err,
				slog.String("categoryId", item.CategoryID),
				slog.Int("qty", item.Qty))
			return fmt.Errorf("hold stock: %w", err)
		}
		if tag.RowsAffected() == 0 {
			dbLog(ctx, r.log, "stock.hold", qHold, start, fmt.Errorf("stock unavailable"),
				slog.String("categoryId", item.CategoryID),
				slog.Int("qty", item.Qty))
			return apperror.StockUnavailable()
		}
		dbLog(ctx, r.log, "stock.hold", qHold, start, nil,
			slog.String("categoryId", item.CategoryID),
			slog.Int("qty", item.Qty))
	}
	return nil
}

// ReleaseStock mengembalikan hold (available += qty) dalam transaksi.
func (r *StockRepository) ReleaseStock(ctx context.Context, tx outbound.Tx, bookingID string) error {
	start := time.Now()
	const qRelease = `
		UPDATE ticket_category tc
		   SET available = available + sub.qty
		FROM (
			SELECT category_id, COUNT(*) AS qty
			FROM booking_item WHERE booking_id = $1
			GROUP BY category_id
		) sub
		WHERE tc.id = sub.category_id`
	_, err := asTx(tx).Exec(ctx, qRelease, bookingID)
	dbLog(ctx, r.log, "stock.release", qRelease, start, err, slog.String("bookingId", bookingID))
	return err
}

// GetAvailableByEvent mengembalikan snapshot stok per kategori.
func (r *StockRepository) GetAvailableByEvent(ctx context.Context, eventID string) ([]entity.CategoryStock, error) {
	start := time.Now()
	const qAvail = `SELECT id, name, available FROM ticket_category WHERE event_id = $1`
	rows, err := r.pool.Query(ctx, qAvail, eventID)
	if err != nil {
		dbLog(ctx, r.log, "stock.get_available_by_event", qAvail, start, err, slog.String("eventId", eventID))
		return nil, err
	}
	defer rows.Close()
	var result []entity.CategoryStock
	for rows.Next() {
		var c entity.CategoryStock
		if err := rows.Scan(&c.CategoryID, &c.Name, &c.Available); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	dbLog(ctx, r.log, "stock.get_available_by_event", qAvail, start, nil,
		slog.String("eventId", eventID),
		slog.Int("categories", len(result)))
	return result, nil
}

// GetPriceMap mengembalikan map[categoryID]price untuk price_snapshot booking_item.
func (r *StockRepository) GetPriceMap(ctx context.Context, eventID string) (map[string]int64, error) {
	start := time.Now()
	const qPriceMap = `SELECT id, price FROM ticket_category WHERE event_id = $1`
	rows, err := r.pool.Query(ctx, qPriceMap, eventID)
	if err != nil {
		dbLog(ctx, r.log, "stock.get_price_map", qPriceMap, start, err, slog.String("eventId", eventID))
		return nil, fmt.Errorf("get price map: %w", err)
	}
	defer rows.Close()
	m := make(map[string]int64)
	for rows.Next() {
		var id string
		var price int64
		if err := rows.Scan(&id, &price); err != nil {
			return nil, err
		}
		m[id] = price
	}
	dbLog(ctx, r.log, "stock.get_price_map", qPriceMap, start, nil,
		slog.String("eventId", eventID),
		slog.Int("categories", len(m)))
	return m, nil
}

// GetEventName mengembalikan nama event dari tabel event.
// Dipakai untuk mengisi EventName di Kafka payload booking.created.
func (r *StockRepository) GetEventName(ctx context.Context, eventID string) (string, error) {
	start := time.Now()
	const qEventName = `SELECT name FROM event WHERE id = $1`
	var name string
	err := r.pool.QueryRow(ctx, qEventName, eventID).Scan(&name)
	if err != nil {
		dbLog(ctx, r.log, "stock.get_event_name", qEventName, start, err, slog.String("eventId", eventID))
		return "", fmt.Errorf("get event name: %w", err)
	}
	dbLog(ctx, r.log, "stock.get_event_name", qEventName, start, nil,
		slog.String("eventId", eventID),
		slog.String("name", name))
	return name, nil
}

// CountNIKBookings menghitung tiket HELD/PAID per (eventID, nik).
//
// Kuota per-NIK harus atomik: dua transaksi untuk NIK+event yang sama tidak boleh
// sama-sama membaca count lama lalu sama-sama insert (race pada baris yang BELUM ada,
// yang tidak bisa dikunci oleh SELECT ... FOR UPDATE). Karena itu kita pakai
// pg_advisory_xact_lock(eventID, nik) sebagai mutex per-NIK: transaksi kedua menunggu
// sampai yang pertama commit/rollback, lock lepas otomatis di akhir transaksi.
func (r *StockRepository) CountNIKBookings(ctx context.Context, tx outbound.Tx, eventID, nik string) (int, error) {
	start := time.Now()

	// Advisory lock per (eventID, nik) sebagai satu key bigint.
	// pg_advisory_xact_lock punya varian (bigint) dan (int4, int4) — TIDAK ada
	// (bigint, bigint). hashtextextended mengembalikan bigint, jadi kita gabungkan
	// event+nik jadi satu string lalu hash ke satu key bigint.
	const qLockNIK = `SELECT pg_advisory_xact_lock(hashtextextended($1 || ':' || $2, 0))`
	if _, err := asTx(tx).Exec(ctx, qLockNIK, eventID, nik); err != nil {
		dbLog(ctx, r.log, "stock.lock_nik", qLockNIK, start, err,
			slog.String("eventId", eventID))
		return 0, fmt.Errorf("lock nik: %w", err)
	}

	const qCountNIK = `
		SELECT COUNT(*) FROM booking_item bi
		JOIN booking b ON b.id = bi.booking_id
		WHERE b.event_id = $1 AND bi.nik = $2 AND b.status IN ('HELD','PAID')`
	var count int
	err := asTx(tx).QueryRow(ctx, qCountNIK, eventID, nik).Scan(&count)
	dbLog(ctx, r.log, "stock.count_nik_bookings", qCountNIK, start, err,
		slog.String("eventId", eventID),
		slog.Int("count", count))
	if err != nil {
		return 0, fmt.Errorf("count nik bookings: %w", err)
	}
	return count, nil
}
