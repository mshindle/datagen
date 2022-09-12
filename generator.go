package datagen

import (
	"context"
	"sync"
)

// Event represents a first class thingamajig created by a Generator. Serialize() should
// return the output as a byte array.
type Event interface {
	Serialize() ([]byte, error)
}

// Generator represents a data generator which sends an event
// on the supplied channel
type Generator interface {
	Generate() Event
}

// GeneratorFunc type is an adapter to allow the use of ordinary functions as generators. If g is a
// function with the appropriate signature, GeneratorFunc(g) is a Generator that calls g.
type GeneratorFunc func() Event

// Generate calls g() and returns Event
func (g GeneratorFunc) Generate() Event {
	return g()
}

// Publisher represents a data destination to receive the supplied byte slice
type Publisher interface {
	Publish(b []byte)
}

// PublisherFunc adapts a function into the publisher interface
type PublisherFunc func([]byte)

// Publish calls p with argument b
func (p PublisherFunc) Publish(b []byte) {
	p(b)
}

// Engine runs data generation and publishes the generators
type Engine struct {
	generator     Generator
	publisher     Publisher
	numGenerators int
	numPublishers int
}

type Option func(*Engine)

// NewEngine returns a new engine with configured generator & publisher
func NewEngine(generator Generator, publisher Publisher, opts ...Option) *Engine {
	e := &Engine{
		generator:     generator,
		publisher:     publisher,
		numGenerators: 1,
		numPublishers: 1,
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// WithNumGenerators to the number of parallelized generators in use by the engine
func WithNumGenerators(n int) Option {
	return func(e *Engine) {
		e.numGenerators = n
	}
}

// WithNumPublishers to the number of parallelized publishers in use by the engine
func WithNumPublishers(n int) Option {
	return func(e *Engine) {
		e.numPublishers = n
	}
}

// Run starts the data generation, serializes it into the appropriate format,
// and sends the data to the publisher. To cancel the engine running, pass a
// cancellable context to Run.
func (e *Engine) Run(ctx context.Context) {
	// channels for getting the generators to start creating data
	gchans := make([]<-chan Event, 0, e.numGenerators)
	for i := 0; i < e.numGenerators; i++ {
		gchans = append(gchans, e.generate(ctx))
	}

	// fan-in all the generator channels into one...
	events := e.merge(gchans...)

	// channels for getting the publishers to start publishing data
	for i := 0; i < e.numPublishers; i++ {
		e.publish(i, events)
	}
}

func (e *Engine) generate(ctx context.Context) <-chan Event {
	out := make(chan Event)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				out <- e.generator.Generate()
			}
		}
	}()
	return out
}

func (e *Engine) publish(_ int, events <-chan Event) {
	go func() {
		for evt := range events {
			b, err := evt.Serialize()
			if err != nil {
				continue
			}
			e.publisher.Publish(b)
		}
	}()
}

func (e *Engine) merge(gchans ...<-chan Event) <-chan Event {
	var wg sync.WaitGroup
	merged := make(chan Event)

	output := func(gchan <-chan Event) {
		defer wg.Done()
		for g := range gchan {
			merged <- g
		}
	}

	wg.Add(len(gchans))
	for _, gchan := range gchans {
		go output(gchan)
	}

	// close the merged channel when all is done
	go func() {
		wg.Wait()
		close(merged)
	}()

	return merged
}
