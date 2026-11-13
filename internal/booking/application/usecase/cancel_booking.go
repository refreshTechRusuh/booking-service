package usecase

import (
	"context"
	"log/slog"
	"time"

	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/dto"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/entity"
)

// revokedQueueTokenTTL > umur Access Token (5 mnt) sehingga token tak bisa dipakai lagi
// sampai JWT-nya sendiri kedaluwarsa.
const revokedQueueTokenTTL = 10 * time.Minute

// CancelBooking membatalkan booking HELD: user keluar total dari booking room dan harus
// antri ulang. Boleh walau Snap sudah dibuka (payment PENDING); ditolak bila PAID/EXPIRED.
//
// Urutan: (1) DB atomik — status CANCELLED + stok dilepas; (2) cabut queue token supaya
// tidak bisa membuat booking lagi; (3) publish Kafka/SSE di background (best-effort,
// sama seperti jalur expire).
func (u *BookingUsecase) CancelBooking(ctx context.Context, bookingID string) (dto.CancelBookingResponse, error) {
	res, err := u.bookingRepo.CancelBookingTx(ctx, bookingID)
	if err != nil {
		return dto.CancelBookingResponse{}, err
	}
	resp := dto.CancelBookingResponse{BookingID: bookingID, Status: string(entity.BookingStatusCancelled)}
	if res.AlreadyCancelled {
		return resp, nil // idempoten: tidak ada efek samping kedua
	}

	queueToken, err := u.cache.GetBookingQueueToken(ctx, bookingID)
	if err != nil {
		u.log.WarnContext(ctx, "get booking queue token failed", slog.String("bookingId", bookingID), slog.Any("err", err))
	}
	if queueToken != "" {
		if revokeErr := u.cache.RevokeQueueToken(ctx, queueToken, revokedQueueTokenTTL); revokeErr != nil {
			u.log.WarnContext(ctx, "revoke queue token failed", slog.String("bookingId", bookingID), slog.Any("err", revokeErr))
		}
	}

	go u.postReleasePublish(context.WithoutCancel(ctx), bookingID, res.EventID, res.Items, "CANCELLED", queueToken)

	// User sudah membuka Snap: matikan VA/QRIS di Midtrans supaya tidak bisa dibayar
	// setelah stok dilepas. Best-effort; callback yang tetap masuk dijaga payment-service.
	if res.HadPendingPayment && u.paymentCanceller != nil {
		go u.expireGatewayTransaction(context.WithoutCancel(ctx), bookingID)
	}

	u.log.InfoContext(ctx, "booking cancelled",
		slog.String("bookingId", bookingID),
		slog.String("eventId", res.EventID),
		slog.Int("items", len(res.Items)))
	return resp, nil
}

// expireGatewayTransaction meng-expire transaksi Midtrans milik booking yang dibatalkan.
func (u *BookingUsecase) expireGatewayTransaction(ctx context.Context, bookingID string) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := u.paymentCanceller.ExpireTransaction(ctx, bookingID); err != nil {
		u.log.ErrorContext(ctx, "expire midtrans transaction after cancel failed",
			slog.String("bookingId", bookingID), slog.Any("err", err))
	}
}
