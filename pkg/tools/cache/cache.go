package go_cache

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	go_logger "gitlab.com/pietroski-software-company/tools/logger/go-logger/v3/pkg/tools/logger"
	"gitlab.com/pietroski-software-company/tools/options/go-opts/pkg/options"

	off_thread "gitlab.com/pietroski-software-company/lightning-db/pkg/tools/op/off-thread"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate -o ../fakes/go_cache . Cache

type (
	Cacher interface {
		Get(ctx context.Context, key string, target interface{}, fallback Fallback) error
		Set(ctx context.Context, key string, value interface{}) error
		Del(ctx context.Context, key string) error
		DelFromPattern(ctx context.Context, pattern string) error
		DelFromPatternSync(ctx context.Context, pattern string) error
	}
	Fallback func() (interface{}, error)

	InMemoryCache struct {
		logger go_logger.Logger
		store  *sync.Map

		operator off_thread.Operator
	}
)

func New(opts ...options.Option) *InMemoryCache {
	cacher := &InMemoryCache{
		logger: go_logger.NewGoLogger(context.Background(), nil,
			&go_logger.Opts{
				Debug:   false,
				Publish: false,
			}),
		store:    new(sync.Map),
		operator: off_thread.New("cache"),
	}
	options.ApplyOptions(cacher, opts...)

	return cacher
}

func (s *InMemoryCache) Get(ctx context.Context, key string, target interface{}, fallback Fallback) (err error) {
	logger := s.logger.FromCtx(ctx)

	value, ok := s.store.Load(key)
	if !ok {
		logger.Debugf("value not found in cache - calling callback",
			go_logger.Field{"err": err, "key": key})
		if fallback == nil {
			return fmt.Errorf("value not found in cache - key: %v", key)
		}

		value, err = fallback()
		if err != nil {
			return err
		}
	}

	if reflect.ValueOf(target).Elem().CanSet() {
		reflect.ValueOf(target).Elem().Set(reflect.ValueOf(value))
	}

	s.store.Store(key, value)

	return nil
}

func (s *InMemoryCache) Set(_ context.Context, key string, value interface{}) error {
	s.store.Store(key, value)

	return nil
}

func (s *InMemoryCache) Del(_ context.Context, key string) error {
	s.store.Delete(key)

	return nil
}

func (s *InMemoryCache) DelFromPattern(ctx context.Context, pattern string) error {
	logger := s.logger.FromCtx(ctx)
	s.store.Range(func(key, _ interface{}) bool {
		searchKey, ok := key.(string)
		if !ok {
			logger.Debugf("wrong stored key type", go_logger.Field{"key": key})
			s.store.Delete(key)

			return true
		}
		if strings.Contains(searchKey, pattern) {
			s.store.Delete(key)
		}

		return true
	})

	return nil
}

func (s *InMemoryCache) DelFromPatternSync(ctx context.Context, pattern string) error {
	logger := s.logger.FromCtx(ctx)
	s.store.Range(func(key, _ interface{}) bool {
		s.operator.OpX(func() (any, error) {
			searchKey, ok := key.(string)
			if !ok {
				logger.Debugf("wrong stored key type", go_logger.Field{"key": key})
				s.store.Delete(key)

				return nil, nil
			}
			if strings.Contains(searchKey, pattern) {
				s.store.Delete(key)
			}

			return nil, nil
		})

		return true
	})
	if err := s.operator.WaitAndWrapErr(); err != nil {
		return fmt.Errorf("error waiting for cache deletion: %w", err)
	}

	return nil
}
