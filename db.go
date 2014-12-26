package rumcask

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

type DB struct {
	dir     string
	flock   *fileLock
	pages   map[uint32]*Page
	current *Page
	keys    KeyStore

	cLock sync.Mutex
	pLock sync.RWMutex
}

// Open opens a new database in the given directory.
// A new directory will be created if the given path does not exist.
func Open(dir string, keys KeyStore) (*DB, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	flock, err := newFileLock(filepath.Join(dir, "LOCK"))
	if err != nil {
		return nil, err
	}

	db := &DB{
		dir:   dir,
		flock: flock,
		pages: make(map[uint32]*Page),
		keys:  keys,
	}
	if err := db.openPages(); err != nil {
		db.Close()
		return nil, err
	}

	runtime.SetFinalizer(db, (*DB).Close)
	return db, nil
}

// Get retrieves a value from the DB
func (db *DB) Get(key []byte) ([]byte, error) {
	ref, ok := db.keys.Fetch(key)
	if !ok {
		return nil, ERROR_NOT_FOUND
	}

	page := db.page(ref.ID)
	if page == nil {
		return nil, ERROR_NOT_FOUND
	}

	return page.readKey(key, ref.Offset)
}

// Set sets a key, value pair. Returns true if key was replaced,
// or false if the key is new
func (db *DB) Set(key, value []byte) (bool, error) {
	klen, vlen := len(key), len(value)
	if klen < 1 {
		return false, ERROR_KEY_BLANK
	} else if klen > MAX_KEY_LEN {
		return false, ERROR_KEY_TOO_LONG
	} else if vlen < 1 {
		return false, ERROR_VALUE_BLANK
	} else if vlen > MAX_VALUE_LEN {
		return false, ERROR_VALUE_TOO_LONG
	}

	db.cLock.Lock()
	defer db.cLock.Unlock()

	offset, err := db.write(key, value)
	if err != nil {
		return false, err
	}

	pref, ok := db.keys.Store(key, PageRef{db.current.id, offset})
	if ok {
		db.page(pref.ID).deleted()
	}
	return ok, nil
}

// Delete deletes a key. Returns true if key was found,
// or false if the key was not stored in the first place
func (db *DB) Delete(key []byte) (bool, error) {
	db.cLock.Lock()
	defer db.cLock.Unlock()

	// Try to remove key, return if not stored
	pref, ok := db.keys.Delete(key)
	if !ok {
		return ok, nil
	}

	page, ok := db.pages[pref.ID]
	if !ok {
		return ok, nil
	}
	return ok, page.delete(pref.Offset)
}

// Close closes the database again
func (db *DB) Close() (err error) {
	defer db.flock.release()

	for _, page := range db.pages {
		if e := page.close(); e != nil {
			err = e
		}
	}
	return
}

// Writes a key/value pair, rotates the current page if needed
func (db *DB) write(key, value []byte) (uint32, error) {
	if !db.current.canWrite(len(key) + len(value)) {
		if err := db.nextPage(); err != nil {
			return 0, err
		}
	}
	return db.current.write(key, value)
}

// Gets the page by ID
func (db *DB) page(id uint32) *Page {
	db.pLock.RLock()
	defer db.pLock.RUnlock()

	return db.pages[id]
}

// Opens all existing pages
func (db *DB) openPages() error {
	names, err := filepath.Glob(filepath.Join(db.dir, "*.rcp"))
	if err != nil {
		return err
	}

	for _, name := range names {
		page, err := openPage(name)
		if err != nil {
			return err
		}
		if err := page.parse(db.keys); err != nil {
			return err
		}
		db.makeCurrent(page)
	}

	if db.current == nil {
		page, err := openPage(db.pageName(0))
		if err != nil {
			return err
		}
		db.makeCurrent(page)
	}
	return nil
}

// Creates a new page and moves the cursor
func (db *DB) nextPage() error {
	page, err := openPage(db.pageName(db.current.id + 1))
	if err != nil {
		return err
	}

	db.makeCurrent(page)
	return nil
}

// Adds a new page to the registry, sets as current
func (db *DB) makeCurrent(page *Page) {
	db.pages[page.id] = page
	db.current = page
}

// Generate a page file name
func (db *DB) pageName(id uint32) string {
	return filepath.Join(db.dir, fmt.Sprintf("%08d.rcp", id))
}
