package process

import (
	"context"
	"errors"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"
	"github.com/go-playground/validator/v10"
	"io"
	"log/slog"
	resp "msgproc/internal/lib/api/response"

	"msgproc/internal/lib/logger/sl"
	"net/http"
)

type Request struct {
	Msg string `json:"msg" validate:"required"`
}

type Response struct {
	resp.Response
	MsgID int64 `json:"msg_id"`
}

type MessageProcessor interface {
	ProcessMsg(ctx context.Context, msg string) (int64, error)
}

func New(log *slog.Logger, processor MessageProcessor) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const op = "http-server.handlers.msg.New"

		log = log.With(
			slog.String("op", op),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		var req Request

		err := render.DecodeJSON(r.Body, &req)
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Error("request body is empty")

				w.WriteHeader(http.StatusBadRequest)
				render.JSON(w, r, resp.Error("empty request"))

				return
			}

			log.Error("failed to decode request body", sl.Err(err))

			w.WriteHeader(http.StatusBadRequest)
			render.JSON(w, r, resp.Error("failed to decode request"))

			return
		}

		log.Info("request body decoded", slog.Any("request", req))

		if err = validator.New().Struct(req); err != nil {
			log.Error("request validation failed", sl.Err(err))

			w.WriteHeader(http.StatusBadRequest)
			render.JSON(w, r, resp.Error("request validation failed"))

			return
		}

		msgID, err := processor.ProcessMsg(r.Context(), req.Msg)
		if err != nil {
			log.Error("failed to process message", sl.Err(err))

			w.WriteHeader(http.StatusInternalServerError)
			render.JSON(w, r, resp.Error("failed to process message"))
		}

		render.JSON(w, r, Response{
			Response: resp.OK(),
			MsgID:    msgID,
		})
	}
}
