// Package db mengimplementasikan outbound.IBookingRepository dan IStockRepository
// menggunakan PostgreSQL via pgx/v5.
package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/entity"
	"github.com/refreshTechRusuh/booking-service/pkg/apperror"
)

// ---- BookingRepository ------------------------------------------------------

type BookingRepository struct{ pool *pgxpool.Pool }

func NewBookingRepository(pool *pgxpool.Pool) *BookingRepository {
	return &BookingRepository{pool: pool}
}

// Create menyimpan booking + booking_items dalam transaksi yang sudah berjalan.
func (r *BookingRepository) Create(ctx context.Context, tx pgx.Tx, b entity.Booking, items []entity.BookingItem) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO booking (id, user_id, event_id, email, status, booking_expires_at, payment_expires_at, created_at)
		VALUES ($1, NULLIF($2,''), $3, $4, $5, $6, $7, $8)`,
		b.ID, b.UserID, b.EventID, b.Email, b.Status, b.BookingExpiresAt, b.PaymentExpiresAt, b.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("booking repo create: %w", err)
	}

	for _, item := range items {
		_, err := tx.Exec(ctx, `
			INSERT INTO booking_item (id, booking_id, category_id, nik, price_snapshot)
			VALUES ($1, $2, $3, $4, $5)`,
			item.ID, item.BookingID, item.CategoryID, item.NIK, item.PriceSnapshot,
		)
		if err != nil {
			return fmt.Errorf("booking item create: %w", err)
		}
	}
	return nil
}

// GetByID mengembalikan booking beserta items-nya.
func (r *BookingRepository) GetByID(ctx context.Context, bookingID string) (entity.Booking, []entity.BookingItem, error) {
	var b entity.Booking
	err := r.pool.QueryRow(ctx, `
		SELECT id, COALESCE(user_id,''), event_id, email, status, booking_expires_at, payment_expires_at, created_at
		FROM booking WHERE id = $1`, bookingID).
		Scan(&b.ID, &b.UserID, &b.EventID, &b.Email, &b.Status, &b.BookingExpiresAt, &b.PaymentExpiresAt, &b.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return entity.Booking{}, nil, apperror.NotFound("booking")
		}
		return entity.Booking{}, nil, fmt.Errorf("booking get by id: %w", err)
	}

	rows, err := r.pool.Query(ctx, `
		SELECT id, booking_id, category_id, nik, price_snapshot
		FROM booking_item WHERE booking_id = $1`, bookingID)
	if err != nil {
		return b, nil, fmt.Errorf("booking item list: %w", err)
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
	return b, items, nil
}

// UpdateStatus memperbarui status booking.
func (r *BookingRepository) UpdateStatus(ctx context.Context, tx pgx.Tx, bookingID string, status entity.BookingStatus) error {
	_, err := tx.Exec(ctx, `UPDATE booking SET status = $1 WHERE id = $2`, status, bookingID)
	return err
}

// ListExpiredHeld mengembalikan booking HELD yang payment_expires_at sudah lewat.
func (r *BookingRepository) ListExpiredHeld(ctx context.Context, limit int) ([]entity.Booking, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, COALESCE(user_id,''), event_id, email, status, booking_expires_at, payment_expires_at, created_at
		FROM booking
		WHERE status = 'HELD' AND payment_expires_at < NOW()
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
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
	return result, nil
}

// ---- StockRepository --------------------------------------------------------

type StockRepository struct{ pool *pgxpool.Pool }

func NewStockRepository(pool *pgxpool.Pool) *StockRepository {
	return &StockRepository{pool: pool}
}

// HoldStock mengurangi available atomic.
// Jika ada category yang 0 row terpengaruh → rollback dan return StockUnavailable.
func (r *StockRepository) HoldStock(ctx context.Context, tx pgx.Tx, items []entity.HoldItem) error {
	for _, item := range items {
		tag, err := tx.Exec(ctx, `
			UPDATE ticket_category
			   SET available = available - $1
			 WHERE id = $2 AND available >= $1`,
			item.Qty, item.CategoryID)
		if err != nil {
			return fmt.Errorf("hold stock exec: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return apperror.StockUnavailable()
		}
	}
	return nil
}

// ReleaseStock mengembalikan hold stok saat booking expired.
func (r *StockRepository) ReleaseStock(ctx context.Context, tx pgx.Tx, bookingID string) error {
	_, err := tx.Exec(ctx, `
		UPDATE ticket_category tc
		   SET available = available + bi.qty
		FROM (
			SELECT category_id, COUNT(*) AS qty
			FROM booking_item WHERE booking_id = $1
			GROUP BY category_id
		) bi
		WHERE tc.id = bi.category_id`, bookingID)
	return err
}

// GetAvailableByEvent mengembalikan snapshot stok per kategori.
func (r *StockRepository) GetAvailableByEvent(ctx context.Context, eventID string) ([]entity.CategoryStock, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, name, available FROM ticket_category WHERE event_id = $1`, eventID)
	if err != nil {
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
	return result, nil
}

// CountNIKBookings menghitung tiket aktif/terbayar per (eventID, nik) dengan FOR UPDATE.
func (r *StockRepository) CountNIKBookings(ctx context.Context, tx pgx.Tx, eventID, nik string) (int, error) {
	var count int
	err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM booking_item bi
		JOIN booking b ON b.id = bi.booking_id
		WHERE b.event_id = $1 AND bi.nik = $2 AND b.status IN ('HELD','PAID')
		FOR UPDATE`, eventID, nik).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count nik bookings: %w", err)
	}
	return count, nil
}
