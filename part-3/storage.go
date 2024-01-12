package raft

import (
	"sync"
)

// Storage is an interface implemented by stable storage providers
type Storage interface {
	Set(key string, value []byte)

	Get(key string) ([]byte, bool)

	// HasData returns true ifff any Sets were made on this Storage
	// Basically like a !Empty check
	HasData() bool
}

// MapStorage is an in-memory implementation of Storage interface
type MapStorage struct {
	mu sync.Mutex
	m  map[string][]byte
}

func NewMapStorage() *MapStorage {
	m := make(map[string][]byte)
	return &MapStorage{
		m: m,
	}
}

// Now this implementation should adhere to Storage interface

func (ms *MapStorage) Get(key string) ([]byte, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	v, found := ms.m[key]
	return v, found
}

func (ms *MapStorage) Set(key string, value []byte) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.m[key] = value
}

func (ms *MapStorage) HasData() bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.m) > 0
}
