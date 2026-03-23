package repositories

import (
	"context"
	"fmt"
	"time"

	"github.com/karlseguin/ccache/v3"

	"transfers-api/internal/config"
	"transfers-api/internal/enums"
	"transfers-api/internal/known_errors"
	"transfers-api/internal/models"
)

type TransfersCcacheRepo struct {
	client     *ccache.Cache[transferCCacheDAO]
	ttlSeconds int32
}

type transferCCacheDAO struct {
	ID         string  `json:"id"`
	SenderID   string  `json:"sender_id"`
	ReceiverID string  `json:"receiver_id"`
	Currency   string  `json:"currency"`
	Amount     float64 `json:"amount"`
	State      string  `json:"state"`
}

func NewTransfersCcachedRepository(cfg config.CCache) *TransfersCcacheRepo {
	client := ccache.New(
		ccache.Configure[transferCCacheDAO]().
			MaxSize(cfg.MaxSize),
	)

	return &TransfersCcacheRepo{
		client:     client,
		ttlSeconds: int32(cfg.TTLSeconds),
	}
}

func (r *TransfersCcacheRepo) Create(ctx context.Context, transfer models.Transfer) (string, error) {
	if transfer.ID == "" {
		return "", fmt.Errorf("transfer ID required for cache create")
	}

	dao := transferCCacheDAO{
		ID:         transfer.ID,
		SenderID:   transfer.SenderID,
		ReceiverID: transfer.ReceiverID,
		Currency:   transfer.Currency.String(),
		Amount:     transfer.Amount,
		State:      transfer.State,
	}

	r.client.Set(
		transfer.ID,
		dao,
		time.Duration(r.ttlSeconds)*time.Second,
	)

	return transfer.ID, nil
}

func (r *TransfersCcacheRepo) GetByID(ctx context.Context, id string) (models.Transfer, error) {
	item := r.client.Get(id)
	if item == nil || item.Expired() {
		if item != nil && item.Expired() {
			r.client.Delete(id)
		}

		return models.Transfer{}, fmt.Errorf("transfer not found: %w", known_errors.ErrNotFound)
	}

	dao := item.Value()

	return models.Transfer{
		ID:         dao.ID,
		SenderID:   dao.SenderID,
		ReceiverID: dao.ReceiverID,
		Currency:   enums.ParseCurrency(dao.Currency),
		Amount:     dao.Amount,
		State:      dao.State,
	}, nil
}

func (r *TransfersCcacheRepo) Update(ctx context.Context, transfer models.Transfer) error {
	item := r.client.Get(transfer.ID)
	if item == nil {
		return fmt.Errorf("transfer not found: %w", known_errors.ErrNotFound)
	}

	dao := item.Value()

	if transfer.SenderID != "" {
		dao.SenderID = transfer.SenderID
	}

	if transfer.ReceiverID != "" {
		dao.ReceiverID = transfer.ReceiverID
	}

	if transfer.Currency != enums.CurrencyUnknown {
		dao.Currency = transfer.Currency.String()
	}

	if transfer.Amount != 0 {
		dao.Amount = transfer.Amount
	}

	if transfer.State != "" {
		dao.State = transfer.State
	}

	r.client.Set(
		transfer.ID,
		dao,
		time.Duration(r.ttlSeconds)*time.Second,
	)

	return nil
}

func (r *TransfersCcacheRepo) Delete(ctx context.Context, id string) error {
	item := r.client.Get(id)
	if item == nil {
		return fmt.Errorf("transfer not found: %w", known_errors.ErrNotFound)
	}

	r.client.Delete(id)
	return nil
}
