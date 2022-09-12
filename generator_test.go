package datagen

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type message string

func (m message) Serialize() ([]byte, error) {
	return []byte(m), nil
}

func TestEngine_Run(t *testing.T) {
	const ttl = 25
	const step = 10
	const ngen = 2
	var want uint32 = (ttl / step) * ngen
	var ops uint32

	g := GeneratorFunc(func() Event {
		time.Sleep(step * time.Millisecond)
		return message("lorem ipsum")
	})
	p := PublisherFunc(func(b []byte) { atomic.AddUint32(&ops, 1) })

	engine := NewEngine(g, p, WithNumGenerators(ngen), WithNumPublishers(ngen))
	ctx, cancel := context.WithTimeout(context.Background(), ttl*time.Millisecond)
	defer cancel()

	engine.Run(ctx)
	for _ = range ctx.Done() {
		fmt.Println("waiting for end....")
	}
	if ops != want {
		t.Errorf("engine not cancelled properly. got: %d, want: %d", ops, want)
	}
}
