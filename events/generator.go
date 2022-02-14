package events

import (
	"sync"
)

// Event represents a generated events from a Generator. Serialize() should
// return the events as an array of bytes.

type Event interface {
	Serialize() ([]byte, error)
}

// Generator represents a data generator which sends an events
// along the supplied channel
type Generator interface {
	Generate() Event
}

// GeneratorFunc type is an adapter to allow the use of ordinary functions as generators. If g is a function with
// the appropriate signature, GeneratorFunc(g) is a Generator that calls g.
type GeneratorFunc func() Event

// Generate calls g() and returns Event
func (g GeneratorFunc) Generate() Event {
	return g()
}

// Publisher represents a data destination to receive the supplied byte slice
type Publisher interface {
	Publish(b []byte)
}

// Engine runs data generation and publishes the events
type Engine struct {
	generator     Generator
	publisher     Publisher
	numGenerators int
	numPublishers int
}

// New returns a new engine with configured generator & publisher
func New(generator Generator, publisher Publisher) *Engine {
	return &Engine{
		generator:     generator,
		publisher:     publisher,
		numGenerators: 1,
		numPublishers: 1,
	}
}

// WithGenerators to the number of parallelized generators in use by the engine
func (e *Engine) WithGenerators(n int) *Engine {
	e.numGenerators = n
	return e
}

// WithPublishers to the number of parallelized publishers in use by the engine
func (e *Engine) WithPublishers(n int) *Engine {
	e.numPublishers = n
	return e
}

// Run starts the data generation, serializes it into the appropriate format,
// and sends the data to the publisher. To stop data generation, pass any bool
// value to the returned channel.
func (e *Engine) Run() (chan<- bool, error) {
	// send our exit notice
	done := make(chan bool)

	// channels for getting the generators to start creating data
	gchans := make([]<-chan Event, 0, e.numGenerators)
	for i := 0; i < e.numGenerators; i++ {
		gchans = append(gchans, e.generate(done))
	}

	// fan-in all the generator channels into one...
	events := e.merge(gchans...)

	// channels for getting the publishers to start publishing data
	for i := 0; i < e.numPublishers; i++ {
		e.publish(i, events)
	}

	return done, nil
}

func (e *Engine) generate(done <-chan bool) <-chan Event {
	out := make(chan Event)
	go func() {
		defer close(out)
		for {
			select {
			case <-done:
				return
			default:
				out <- e.generator.Generate()
			}
		}
	}()
	return out
}

func (e *Engine) publish(id int, events <-chan Event) {
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
