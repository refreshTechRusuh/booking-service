package dto_test

import (
	"testing"

	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/dto"
	"github.com/stretchr/testify/assert"
)

// ── ToHoldItems ────────────────────────────────────────────────────────────────

func TestCreateBookingRequest_ToHoldItems_singleCategory(t *testing.T) {
	req := dto.CreateBookingRequest{
		Items: []dto.BookingItem{
			{CategoryID: "cat-vip", Attendees: []dto.AttendeeInput{
				{NIK: "3273000000000001"},
				{NIK: "3273000000000002"},
			}},
		},
	}
	items := req.ToHoldItems()
	assert.Len(t, items, 1)
	assert.Equal(t, "cat-vip", items[0].CategoryID)
	assert.Equal(t, 2, items[0].Qty)
}

func TestCreateBookingRequest_ToHoldItems_multipleCategories(t *testing.T) {
	req := dto.CreateBookingRequest{
		Items: []dto.BookingItem{
			{CategoryID: "cat-vip", Attendees: []dto.AttendeeInput{{NIK: "111"}}},
			{CategoryID: "cat-fest", Attendees: []dto.AttendeeInput{{NIK: "222"}, {NIK: "333"}}},
		},
	}
	items := req.ToHoldItems()
	assert.Len(t, items, 2)

	qtyMap := make(map[string]int)
	for _, it := range items {
		qtyMap[it.CategoryID] = it.Qty
	}
	assert.Equal(t, 1, qtyMap["cat-vip"])
	assert.Equal(t, 2, qtyMap["cat-fest"])
}

func TestCreateBookingRequest_ToHoldItems_empty(t *testing.T) {
	req := dto.CreateBookingRequest{}
	items := req.ToHoldItems()
	assert.Empty(t, items)
}

// ── TotalTickets ───────────────────────────────────────────────────────────────

func TestCreateBookingRequest_TotalTickets_one(t *testing.T) {
	req := dto.CreateBookingRequest{
		Items: []dto.BookingItem{
			{CategoryID: "cat-1", Attendees: []dto.AttendeeInput{{NIK: "111"}}},
		},
	}
	assert.Equal(t, 1, req.TotalTickets())
}

func TestCreateBookingRequest_TotalTickets_three(t *testing.T) {
	req := dto.CreateBookingRequest{
		Items: []dto.BookingItem{
			{CategoryID: "cat-1", Attendees: []dto.AttendeeInput{{NIK: "111"}, {NIK: "222"}}},
			{CategoryID: "cat-2", Attendees: []dto.AttendeeInput{{NIK: "333"}}},
		},
	}
	assert.Equal(t, 3, req.TotalTickets())
}

func TestCreateBookingRequest_TotalTickets_empty(t *testing.T) {
	req := dto.CreateBookingRequest{}
	assert.Equal(t, 0, req.TotalTickets())
}

// ── NIKQtyMap ─────────────────────────────────────────────────────────────────

func TestCreateBookingRequest_NIKQtyMap_oneNIK(t *testing.T) {
	req := dto.CreateBookingRequest{
		Items: []dto.BookingItem{
			{CategoryID: "cat-1", Attendees: []dto.AttendeeInput{{NIK: "3273000000000001"}}},
		},
	}
	m := req.NIKQtyMap()
	assert.Equal(t, 1, m["3273000000000001"])
}

func TestCreateBookingRequest_NIKQtyMap_duplicateNIK(t *testing.T) {
	// NIK yang sama di dua kategori berbeda
	req := dto.CreateBookingRequest{
		Items: []dto.BookingItem{
			{CategoryID: "cat-1", Attendees: []dto.AttendeeInput{{NIK: "3273000000000001"}}},
			{CategoryID: "cat-2", Attendees: []dto.AttendeeInput{{NIK: "3273000000000001"}}},
		},
	}
	m := req.NIKQtyMap()
	assert.Equal(t, 2, m["3273000000000001"])
}

func TestCreateBookingRequest_NIKQtyMap_multipleNIKs(t *testing.T) {
	req := dto.CreateBookingRequest{
		Items: []dto.BookingItem{
			{CategoryID: "cat-1", Attendees: []dto.AttendeeInput{
				{NIK: "3273000000000001"},
				{NIK: "3273000000000002"},
			}},
		},
	}
	m := req.NIKQtyMap()
	assert.Equal(t, 1, m["3273000000000001"])
	assert.Equal(t, 1, m["3273000000000002"])
}

// ── PriceSnapshot ─────────────────────────────────────────────────────────────

func TestCreateBookingRequest_PriceSnapshot_returnsZero(t *testing.T) {
	req := dto.CreateBookingRequest{}
	// PriceSnapshot adalah placeholder — selalu return 0 (TODO: inject dari event-service)
	price := req.PriceSnapshot("cat-vip")
	assert.Equal(t, int64(0), price)
}
