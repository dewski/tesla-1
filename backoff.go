package tesla

import (
	"time"

	"github.com/cenkalti/backoff"
)

func newExponentialBackOff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 5 * time.Second
	b.Multiplier = 2.0
	b.MaxInterval = 320 * time.Second
	b.Reset()

	return b
}
