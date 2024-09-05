package cli

import (
	"context"
	"os"
	"os/signal"

	"github.com/gptscript-ai/cmd"
)

func Main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	cmd.MainCtx(ctx, New())
}
