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
	"github.com/jackc/pgx/v5/pgxpool"
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
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewBookingRepository(pool *pgxpool.Pool, log *slog.Logger) *BookingRepository {
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
		VALUES ($1, NULLIF($2,''), $3, $4, $5, $6, $7, $8)`
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

	if _, err := tx.Exec(ctx, qExpireBooking, bookingID); err != nil {
		dbLog(ctx, r.log, "booking.expire_tx.update_booking", qExpireBooking, start, err, slog.String("bookingId", bookingID))
		return nil, fmt.Errorf("expire booking update: %w", err)
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

type StockRepository struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewStockRepository(pool *pgxpool.Pool, log *slog.Logger) *StockRepository {
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

// CountNIKBookings menghitung tiket HELD/PAID per (eventID, nik) — SELECT FOR UPDATE.
func (r *StockRepository) CountNIKBookings(ctx context.Context, tx outbound.Tx, eventID, nik string) (int, error) {
	start := time.Now()
	const qCountNIK = `
		SELECT COUNT(*) FROM booking_item bi
		JOIN booking b ON b.id = bi.booking_id
		WHERE b.event_id = $1 AND bi.nik = $2 AND b.status IN ('HELD','PAID')
		FOR UPDATE`
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
