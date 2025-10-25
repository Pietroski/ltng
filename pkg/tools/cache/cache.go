package go_cache

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"gitlab.com/pietroski-software-company/golang/devex/options"
	"gitlab.com/pietroski-software-company/golang/devex/slogx"
	"gitlab.com/pietroski-software-company/golang/devex/syncx"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate
//counterfeiter:generate -o ../fakes/go_cache . Cacher

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
		logger slogx.SLogger
		store  *sync.Map

		operator syncx.Operator
	}
)

func New(opts ...options.Option) *InMemoryCache {
	cacher := &InMemoryCache{
		logger:   slogx.New(),
		store:    new(sync.Map),
		operator: syncx.NewThreadOperator("cache"),
	}
	options.ApplyOptions(cacher, opts...)

	return cacher
}

func (s *InMemoryCache) Get(ctx context.Context, key string, target interface{}, fallback Fallback) (err error) {
	value, ok := s.store.Load(key)
	if !ok {
		s.logger.Debug(ctx, "value not found in cache - calling callback", "key", key, "err", err)
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
	s.store.Range(func(key, _ interface{}) bool {
		searchKey, ok := key.(string)
		if !ok {
			s.logger.Debug(ctx, "wrong stored key type", "key", key)
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
	s.store.Range(func(key, _ interface{}) bool {
		s.operator.OpX(func() (any, error) {
			searchKey, ok := key.(string)
			if !ok {
				s.logger.Debug(ctx, "wrong stored key type", "key", key)
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
