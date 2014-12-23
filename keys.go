package rumcask

import "sync"

// KeyStore is the interface for a keystore.
// Please see HashKeyStore for a simple, non-iterable,
// in-memory implementation
type KeyStore interface {
	// Store stores the key, referencing the given
	// pageID and the page offset.
	// Returns true and the previous PageRef if
	// the key was stored before.
	Store(key []byte, ref PageRef) (PageRef, bool)

	// Delete deletes a key.
	// Returns true and the previous PageRef if
	// the key was stored before.
	Delete(key []byte) (PageRef, bool)

	// Fetch retrieves the PageRef for a key, returns false
	// if not found.
	Fetch(key []byte) (PageRef, bool)
}

// A HashKeyStore is the simples KeyStore implementation.
// Keys are non-iterable and are held in memory all the time.
type HashKeyStore struct {
	refs map[string]PageRef
	lock sync.Mutex
}

// NewHashKeyStore creates a new, empty HashKeyStore
func NewHashKeyStore() *HashKeyStore {
	return &HashKeyStore{refs: make(map[string]PageRef)}
}

func (s *HashKeyStore) Fetch(key []byte) (PageRef, bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ref, ok := s.refs[string(key)]
	return ref, ok
}

func (s *HashKeyStore) Store(key []byte, ref PageRef) (PageRef, bool) {
	skey := string(key)

	s.lock.Lock()
	defer s.lock.Unlock()

	prev, ok := s.refs[skey]
	s.refs[skey] = ref
	return prev, ok
}

func (s *HashKeyStore) Delete(key []byte) (PageRef, bool) {
	skey := string(key)

	s.lock.Lock()
	defer s.lock.Unlock()

	prev, ok := s.refs[skey]
	delete(s.refs, skey)
	return prev, ok
}
