package msgproc

import (
	"context"
	"fmt"
	"log/slog"
	"msgproc/internal/lib/logger/sl"
)

type MsgProc struct {
	log       *slog.Logger
	MsgSaver  MsgSaver
	MsgSender MsgSender
}

type MsgSaver interface {
	SaveMsg(
		ctx context.Context,
		msg string,
	) (int64, error)
}

type MsgSender interface {
	SendMsg(
		ctx context.Context,
		msg string,
		msgID int64,
	) error
}

func New(
	log *slog.Logger,
	msgSaver MsgSaver,
	msgSender MsgSender,
) *MsgProc {
	return &MsgProc{
		log:       log,
		MsgSaver:  msgSaver,
		MsgSender: msgSender,
	}
}

func (m *MsgProc) ProcessMsg(
	ctx context.Context,
	msg string,
) (int64, error) {
	const op = "services.msgproc.ProcessMsg"

	log := m.log.With(
		slog.String("op", op),
	)

	log.Info("processing new message")

	msgID, err := m.MsgSaver.SaveMsg(ctx, msg)
	if err != nil {
		log.Error("failed to save message", sl.Err(err))

		return 0, fmt.Errorf("%s: %w", op, err)
	}
	err = m.MsgSender.SendMsg(ctx, msg, msgID)
	if err != nil {
		log.Error("failed to send message", sl.Err(err))

		return 0, fmt.Errorf("%s: %w", op, err)
	}

	log.Info("message processed successfully")

	return msgID, nil
}
