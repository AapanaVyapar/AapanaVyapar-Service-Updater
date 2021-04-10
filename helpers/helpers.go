package helpers

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
)

func ContextError(ctx context.Context) error {

	switch ctx.Err() {
	case context.Canceled:
		log.Println("Request Canceled")
		return status.Error(codes.DeadlineExceeded, "Request Canceled")
	case context.DeadlineExceeded:
		log.Println("DeadLine Exceeded")
		return status.Error(codes.DeadlineExceeded, "DeadLine Exceeded")
	default:
		return nil
	}
}
