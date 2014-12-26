package btree

import (
	"bytes"
	"sync"

	"github.com/bsm/rumcask"
)

type pair struct {
	K []byte
	R rumcask.PageRef
}

func (kv *pair) Less(than bItem) bool {
	return bytes.Compare(kv.K, than.(*pair).K) < 0
}

// Iterator allows callers to iterate the key/ref pairs
// in lexical order. When this function returns false,
// iteration will stop immediately.
type Iterator func(key []byte, ref rumcask.PageRef) bool

// A btree based KeyStore implementation.
// Keys are iterable and are held in memory.
type KeyStore struct {
	tree *bTree
	lock sync.RWMutex
}

// NewKeyStore creates a new, empty BTree key store
func NewKeyStore(degree int) *KeyStore {
	return &KeyStore{tree: newTree(degree)}
}

// Fetch retrieves the ref at key
func (s *KeyStore) Fetch(key []byte) (_ rumcask.PageRef, _ bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	item := s.tree.Get(&pair{K: key})
	if item == nil {
		return
	}
	return item.(*pair).R, true
}

// Store stores a key/ref pair
func (s *KeyStore) Store(key []byte, ref rumcask.PageRef) (_ rumcask.PageRef, _ bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	item := s.tree.ReplaceOrInsert(&pair{key, ref})
	if item == nil {
		return
	}
	return item.(*pair).R, true
}

// Delete deletes a key
func (s *KeyStore) Delete(key []byte) (_ rumcask.PageRef, _ bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	item := s.tree.Delete(&pair{K: key})
	if item == nil {
		return
	}
	return item.(*pair).R, true
}

// Len returns the number of keys in the store
func (s *KeyStore) Len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.tree.Len()
}

// Iterate iterates over a range of keys >= min and < max
func (s *KeyStore) Iterate(min, max []byte, each Iterator) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	s.tree.AscendRange(&pair{K: min}, &pair{K: max}, func(item bItem) bool {
		kv := item.(*pair)
		return each(kv.K, kv.R)
	})
}
