package models

type Statistics struct {
	TotalMessages          int64
	MessagesByStatus       map[string]int64
	MessagesLastDay        int64
	MessagesUpdatedLastDay int64
	AverageMessageLength   float64
}
