package rumcask

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"
)

// Page stats
type PageStats struct {
	// Approximate number of total entries
	// written to the page
	Written uint32
	// Approximate number of deleted entries
	// in the page
	Deleted uint32
}

func (s *PageStats) decode(b []byte) {
	if len(b) > 3 {
		s.Written = binLE.Uint32(b[0:])
	}
	if len(b) > 7 {
		s.Deleted = binLE.Uint32(b[4:])
	}
}

func (s *PageStats) encode() []byte {
	buf := make([]byte, 8)
	binLE.PutUint32(buf[0:], s.Written)
	binLE.PutUint32(buf[4:], s.Deleted)
	return buf
}

// Each page starts with a page header:
//
// 	MAGIC WORD        7 bytes
// 	VERSION           1 byte
// 	PAGE STATS        8 bytes
// 	RESERVED SPACE  120 bytes
//
type pageHeader struct {
	Version uint8
	Stats   PageStats
}

func (h *pageHeader) read(r io.ReaderAt) error {
	buf := make([]byte, PAGE_HEADER_LEN)
	if _, err := r.ReadAt(buf, 0); err != nil {
		return err
	} else if !bytes.Equal(_MAGIC, buf[:7]) {
		return ERROR_PAGE_BAD_HEADER
	} else if h.Version = buf[7]; h.Version != VERSION {
		return ERROR_PAGE_BAD_HEADER
	}
	(&h.Stats).decode(buf[8:16])
	return nil
}

func (h *pageHeader) write(w io.WriterAt) error {
	buf := make([]byte, PAGE_HEADER_LEN)
	copy(buf[0:], _MAGIC)
	buf[7] = VERSION
	copy(buf[8:], (&h.Stats).encode())
	_, err := w.WriteAt(buf, 0)
	return err
}

func (h *pageHeader) writeStats(w io.WriterAt) error {
	buf := (&h.Stats).encode()
	_, err := w.WriteAt(buf, 8)
	return err
}

func (h *pageHeader) recWritten() { atomic.AddUint32(&h.Stats.Written, 1) }
func (h *pageHeader) recDeleted() { atomic.AddUint32(&h.Stats.Deleted, 1) }

// PageRef identifies the page file and an offset position
type PageRef struct {
	ID     uint32
	Offset uint32
}

const (
	OH_KEY  = 2
	OH_VAL  = 4
	OH_CSUM = 2
	OH_KV   = OH_KEY + OH_VAL
	OH_FULL = OH_KV + OH_CSUM
)

// An individual page-file
// Pages are not thread-safe. Locks are implemented on DB level
type Page struct {
	header *pageHeader
	id     uint32
	offset uint32
	file   *os.File

	closer, eoloop chan struct{}
}

func openPage(fname string) (*Page, error) {
	base := filepath.Base(fname)
	bext := filepath.Ext(base)
	id, err := strconv.ParseUint(base[:len(base)-len(bext)], 10, 32)
	if err != nil {
		return nil, ERROR_PAGE_INVALID
	}

	file, err := os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		return nil, err
	}

	offset, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		file.Close()
		return nil, err
	}

	page := &Page{
		id:     uint32(id),
		header: &pageHeader{Version: VERSION},
		file:   file,
		offset: uint32(offset),
		closer: make(chan struct{}),
		eoloop: make(chan struct{}),
	}
	if page.offset == 0 {
		if err := page.header.write(file); err != nil {
			file.Close()
			return nil, err
		}
		page.offset = PAGE_HEADER_LEN
	} else if err := page.header.read(file); err != nil {
		file.Close()
		return nil, err
	}

	go page.loop()
	return page, nil
}

// reads known key from offset
func (p *Page) readKey(key []byte, offset uint32) ([]byte, error) {
	klen := uint32(len(key))
	blen := make([]byte, OH_VAL)
	if _, err := p.file.ReadAt(blen, int64(offset)+OH_KEY); err != nil {
		return nil, err
	}

	vlen := int(binLE.Uint32(blen))
	if vlen > MAX_VALUE_LEN {
		return nil, ERROR_INVALID_OFFSET
	}

	rest := make([]byte, vlen+OH_CSUM)
	if _, err := p.file.ReadAt(rest, int64(offset+klen+OH_KV)); err != nil {
		return nil, err
	}

	val, csum := rest[:vlen], rest[vlen:]
	if CRC16(append(key, val...)) != binLE.Uint16(csum) {
		return nil, ERROR_INVALID_CHECKSUM
	}
	return val, nil

}

// reads data from the file
func (p *Page) read(offset uint32) ([]byte, []byte, error) {
	lens := make([]byte, OH_KV)
	if _, err := p.file.ReadAt(lens, int64(offset)); err != nil {
		return nil, nil, err
	}

	klen := int(binLE.Uint16(lens[0:]))
	if klen > MAX_KEY_LEN {
		return nil, nil, ERROR_INVALID_OFFSET
	}
	vlen := int(binLE.Uint32(lens[OH_KEY:]))
	if vlen > MAX_VALUE_LEN {
		return nil, nil, ERROR_INVALID_OFFSET
	}

	rest := make([]byte, int(klen+vlen+OH_CSUM))
	if _, err := p.file.ReadAt(rest, int64(offset+OH_KV)); err != nil {
		return nil, nil, err
	}

	pair, csum := rest[:klen+vlen], rest[klen+vlen:]
	if CRC16(pair) != binLE.Uint16(csum) {
		return nil, nil, ERROR_INVALID_CHECKSUM
	}
	return pair[:klen], pair[klen:], nil
}

// appends a key/value to the file
func (p *Page) append(key, value []byte) (uint32, error) {
	klen, vlen := len(key), len(value)

	if klen > MAX_KEY_LEN {
		return 0, ERROR_KEY_TOO_LONG
	} else if vlen > MAX_VALUE_LEN {
		return 0, ERROR_VALUE_TOO_LONG
	}

	kvlen := klen + vlen
	data := make([]byte, 8+kvlen)
	binLE.PutUint16(data[0:], uint16(klen))
	binLE.PutUint32(data[OH_KEY:], uint32(vlen))
	copy(data[OH_KV:], key)
	copy(data[OH_KV+klen:], value)
	binLE.PutUint16(data[OH_KV+kvlen:], CRC16(data[OH_KV:OH_KV+kvlen]))

	offset := p.pos()
	n, err := p.file.WriteAt(data, int64(offset))
	if err != nil {
		return 0, err
	}
	atomic.AddUint32(&p.offset, uint32(n))
	p.header.recWritten()
	return offset, nil
}

// increment delete stats
func (p *Page) deleted() {
	if p != nil {
		p.header.recDeleted()
	}
}

// Parse page, merge keys
func (p *Page) parse(store KeyStore) (err error) {
	var key, val []byte
	pos := uint32(PAGE_HEADER_LEN)
	for err == nil {
		if key, val, err = p.read(pos); err == nil {
			store.Store(key, PageRef{p.id, pos})
			pos += uint32(len(key)+len(val)) + OH_FULL
		}
	}
	if err == io.EOF {
		err = nil
	}
	return
}

// returns true if there is not enough space
// to write the next key/value
func (p *Page) canWrite(kvlen int) bool {
	return p.pos()+uint32(kvlen)+8 < MAX_PAGE_SIZE
}

// returns current position (atomic)
func (p *Page) pos() uint32 {
	return atomic.LoadUint32(&p.offset)
}

// unlinks the page completely
func (p *Page) unlink() error {
	fname := p.file.Name()
	p.close()
	return os.Remove(fname)
}

// close closes the file
func (p *Page) close() error {
	select {
	case _, open := <-p.closer:
		if !open {
			return nil
		}
	default:
	}

	close(p.closer)
	<-p.eoloop // wait for loop to exit
	return p.file.Close()
}

// stats persistence loop
func (p *Page) loop() {
	defer func() {
		p.header.writeStats(p.file)
		close(p.eoloop)
	}()

	for {
		select {
		case <-p.closer:
			return
		case <-time.After(time.Second):
			// wait for 1s
		}
		p.header.writeStats(p.file)
	}
}
