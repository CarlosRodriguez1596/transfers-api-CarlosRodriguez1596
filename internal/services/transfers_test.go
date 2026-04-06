package services

import (
	"context"
	"testing"
	"transfers-api/internal/config"
	"transfers-api/internal/enums"
	"transfers-api/internal/models"
	"transfers-api/internal/services/mocks"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTransfersService_GetByID(t *testing.T) {

	var (
		ctx                = context.Background()
		cfg                = config.BusinessConfig{TransferMinAmount: 1}
		transfersRepo      = mocks.NewTransfersRepositoryMock(t)
		transfersCCache    = mocks.NewTransfersRepositoryMock(t)
		transfersCLocal    = mocks.NewTransfersRepositoryMock(t)
		transfersPublisher = mocks.NewTransfersPublisherMock(t)
	)

	for _, tc := range []struct {
		name             string
		transferID       string
		expectedTransfer models.Transfer
	}{
		{
			name:       "Transfer successfully retrieved",
			transferID: "Test-1",
			expectedTransfer: models.Transfer{
				ID:         "Test-1",
				SenderID:   "Sender-123",
				ReceiverID: "Receiver-456",
				Amount:     100,
				Currency:   enums.CurrencyUSD,
			},
		},
		{
			name:       "Transfer successfully retrieved",
			transferID: "Test-2",
			expectedTransfer: models.Transfer{
				ID:         "Test-2",
				SenderID:   "Sender-123",
				ReceiverID: "Receiver-456",
				Amount:     100,
				Currency:   enums.CurrencyUSD,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {

			transfersCLocal.
				On("GetByID", ctx, tc.transferID).
				Return(tc.expectedTransfer, nil).
				Once()

			svc := NewTransfersService(cfg, transfersRepo, transfersCCache, transfersCLocal, transfersPublisher)

			transfer, err := svc.GetByID(ctx, tc.transferID)

			assert.NoError(t, err)
			assert.Equal(t, tc.transferID, transfer.ID)
		})
	}
}

func TestTransfersService_Create(t *testing.T) {
	ctx := context.Background()
	cfg := config.BusinessConfig{TransferMinAmount: 1}

	transfersRepo := mocks.NewTransfersRepositoryMock(t)
	transfersCCache := mocks.NewTransfersRepositoryMock(t)
	transfersCLocal := mocks.NewTransfersRepositoryMock(t)
	transfersPublisher := mocks.NewTransfersPublisherMock(t)

	transfer := models.Transfer{
		SenderID:   "Sender-123",
		ReceiverID: "Receiver-456",
		Amount:     100,
		Currency:   enums.CurrencyUSD,
		State:      "PENDING",
	}

	expectedID := "Test-1"

	transfersRepo.
		On("Create", ctx, transfer).
		Return(expectedID, nil).
		Once()

	transfersCLocal.
		On("Create", ctx, mock.Anything).
		Return(expectedID, nil).
		Once()

	transfersCCache.
		On("Create", ctx, mock.Anything).
		Return(expectedID, nil).
		Once()

	transfersPublisher.
		On("Publish", "create", expectedID).
		Return(nil).
		Maybe()

	svc := NewTransfersService(cfg, transfersRepo, transfersCCache, transfersCLocal, transfersPublisher)

	id, err := svc.Create(ctx, transfer)

	assert.NoError(t, err)
	assert.Equal(t, expectedID, id)
}
