package msgstat

import (
	"context"
	"fmt"
	"log/slog"
	"msgproc/internal/domain/models"
	"msgproc/internal/lib/logger/sl"
)

type StatisticsService struct {
	log     *slog.Logger
	MsgStat MsgStat
}

type MsgStat interface {
	TotalMessages(ctx context.Context) (int64, error)
	MessagesByStatus(ctx context.Context) (map[string]int64, error)
	MessagesLastDay(ctx context.Context) (int64, error)
	MessagesUpdatedLastDay(ctx context.Context) (int64, error)
	AverageMessageLength(ctx context.Context) (float64, error)
}

func New(log *slog.Logger, msgStat MsgStat) *StatisticsService {
	return &StatisticsService{
		log:     log,
		MsgStat: msgStat,
	}
}

func (s *StatisticsService) Stats(ctx context.Context) (*models.Statistics, error) {
	const op = "services.msgstat.TotalMessages"

	log := s.log.With(
		slog.String("op", op),
	)

	log.Info("getting total messages")

	total, err := s.MsgStat.TotalMessages(ctx)
	if err != nil {
		log.Error("failed to get total messages", sl.Err(err))

		return nil, fmt.Errorf("%s: %w", op, err)
	}
	msgByStatus, err := s.MsgStat.MessagesByStatus(ctx)
	if err != nil {
		log.Error("failed to get messages by status", sl.Err(err))
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	msgLastDay, err := s.MsgStat.MessagesLastDay(ctx)
	if err != nil {
		log.Error("failed to get messages last day", sl.Err(err))
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	msgUpdatedLastDay, err := s.MsgStat.MessagesUpdatedLastDay(ctx)
	if err != nil {
		log.Error("failed to get messages updated last day", sl.Err(err))
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	averageMessageLength, err := s.MsgStat.AverageMessageLength(ctx)
	if err != nil {
		log.Error("failed to get average message length", sl.Err(err))
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &models.Statistics{
		TotalMessages:          total,
		MessagesByStatus:       msgByStatus,
		MessagesLastDay:        msgLastDay,
		MessagesUpdatedLastDay: msgUpdatedLastDay,
		AverageMessageLength:   averageMessageLength,
	}, nil
}
