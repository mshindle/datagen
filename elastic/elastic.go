package elastic

import (
	"bytes"
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
)

type Config struct {
	Hosts         []string
	Username      string
	Password      string
	Index         string
	NumWorkers    int
	FlushBytes    int
	FlushInterval time.Duration
}

type Service struct {
	client *elasticsearch.Client
	bi     esutil.BulkIndexer
	ctx    context.Context
	count  uint64
}

func New(ctx context.Context, c Config) (*Service, error) {
	var err error
	p := Service{ctx: ctx}

	p.client, err = NewClient(c)
	if err != nil {
		return nil, err
	}

	p.bi, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         c.Index,
		Client:        p.client,
		NumWorkers:    c.NumWorkers,
		FlushBytes:    c.FlushBytes,
		FlushInterval: c.FlushInterval,
	})
	if err != nil {
		return nil, err
	}

	return &p, err
}

func NewClient(c Config) (*elasticsearch.Client, error) {
	// configure client
	cfg := elasticsearch.Config{
		Addresses:     c.Hosts,
		Username:      c.Username,
		Password:      c.Password,
		RetryOnStatus: []int{429, 502, 503, 504},
		RetryBackoff: func(i int) time.Duration {
			// A simple exponential delay
			d := time.Duration(math.Exp2(float64(i))) * time.Second
			//log.Info().Int("attempt", i).Dur("sleeping", d).Msg("could not connect - backing off...")
			return d
		},
	}
	return elasticsearch.NewClient(cfg)
}

func (s *Service) Publish(b []byte) {
	err := s.bi.Add(
		s.ctx,
		esutil.BulkIndexerItem{
			// Action field configures the operation to perform (index, create, delete, update)
			Action: "index",

			// Body is an `io.Reader` with the payload
			Body: bytes.NewReader(b),

			// OnSuccess is called for each successful operation
			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
				atomic.AddUint64(&s.count, 1)
				if s.count > 0 && s.count%100 == 0 {
					//log.Debug().Uint64("count", s.count).Msg("messages indexed")
				}
			},

			// OnFailure is called for each failed operation
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				//if err != nil {
				//	log.Error().Err(err).Msg("error indexing")
				//} else {
				//	log.Error().
				//		Str("type", res.Error.Type).
				//		Str("reason", res.Error.Reason).
				//		Msg("error indexing")
				//}
			},
		},
	)
	if err != nil {
		//log.Error().Err(err).Msg("unexpected error")
	}
}

func (s *Service) ListIndices() error {
	_, err := s.client.Indices.GetAlias()
	return err
}

func (s *Service) Close() error {
	return s.bi.Close(s.ctx)
}

type BulkResponse struct {
	Errors bool `json:"errors"`
	Items  []struct {
		Index struct {
			ID     string `json:"_id"`
			Result string `json:"result"`
			Status int    `json:"status"`
			Error  struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
				Cause  struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"caused_by"`
			} `json:"error"`
		} `json:"index"`
	} `json:"items"`
}
