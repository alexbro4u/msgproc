package stat

import (
	"context"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"
	"log/slog"
	"msgproc/internal/domain/models"
	resp "msgproc/internal/lib/api/response"
	"msgproc/internal/lib/logger/sl"
	"net/http"
)

type Response struct {
	resp.Response
	models.Statistics
}

type MsgStater interface {
	Stats(ctx context.Context) (*models.Statistics, error)
}

func New(log *slog.Logger, stater MsgStater) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const op = "http-server.handlers.msgstat.New"

		log := log.With(
			slog.String("op", op),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		stats, err := stater.Stats(r.Context())
		if err != nil {
			log.Error("failed to get statistics", sl.Err(err))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		render.JSON(w, r, &Response{
			Response:   resp.OK(),
			Statistics: *stats,
		})
	}
}
