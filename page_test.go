package rumcask

import (
	"bytes"
	"io"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PageStats", func() {
	var subject *PageStats

	BeforeEach(func() {
		subject = new(PageStats)
	})

	It("should encode", func() {
		Expect(subject.encode()).To(Equal([]byte{0, 0, 0, 0, 0, 0, 0, 0}))
		subject.Written = 1001
		subject.Deleted = 501
		Expect(subject.encode()).To(Equal([]byte{233, 3, 0, 0, 245, 1, 0, 0}))
	})

	It("should decode", func() {
		subject.decode(nil)
		Expect(subject).To(Equal(&PageStats{}))
		subject.decode([]byte{233})
		Expect(subject).To(Equal(&PageStats{}))
		subject.decode([]byte{0, 0, 0, 0, 0, 0, 0, 0})
		Expect(subject).To(Equal(&PageStats{}))
		subject.decode([]byte{135, 24, 0, 0, 0, 0, 0})
		Expect(subject).To(Equal(&PageStats{Written: 6279}))
		subject.decode([]byte{233, 3, 0, 0, 245, 1, 0, 0})
		Expect(subject).To(Equal(&PageStats{1001, 501}))
	})

})

var _ = Describe("pageHeader", func() {
	var subject *pageHeader

	BeforeEach(func() {
		subject = &pageHeader{Version: 1, Stats: PageStats{1001, 501}}
	})

	It("should read", func() {
		bin := []byte{}
		Expect(subject.read(bytes.NewReader(bin))).To(Equal(io.EOF))

		bin = make([]byte, 500)
		Expect(subject.read(bytes.NewReader(bin))).To(Equal(ERROR_PAGE_BAD_HEADER))

		copy(bin, []byte{'R', 'U', 'M', 'C', 'A', 'S', 'K'})
		Expect(subject.read(bytes.NewReader(bin))).To(Equal(ERROR_PAGE_BAD_HEADER))

		bin[7] = 2
		Expect(subject.read(bytes.NewReader(bin))).To(Equal(ERROR_PAGE_BAD_HEADER))

		bin[7] = 1
		Expect(subject.read(bytes.NewReader(bin))).NotTo(HaveOccurred())
		Expect(subject).To(Equal(&pageHeader{Version: 1}))

		copy(bin[8:], []byte{233, 3, 0, 0, 245, 1, 0, 0})
		Expect(subject.read(bytes.NewReader(bin))).NotTo(HaveOccurred())
		Expect(subject).To(Equal(&pageHeader{Version: 1, Stats: PageStats{1001, 501}}))
	})

	It("should write", func() {
		buf := make([]byte, 500)
		Expect(subject.write(&writeAtBuffer{buf})).NotTo(HaveOccurred())

		Expect(buf[:8]).To(Equal([]byte{'R', 'U', 'M', 'C', 'A', 'S', 'K', 1}))
		Expect(buf[8:16]).To(Equal([]byte{233, 3, 0, 0, 245, 1, 0, 0}))
		for _, c := range buf[16:] {
			Expect(c).To(Equal(uint8(0)))
		}
	})

	It("should write stats only", func() {
		buf := make([]byte, 500)
		Expect(subject.writeStats(&writeAtBuffer{buf})).NotTo(HaveOccurred())

		Expect(buf[:8]).To(Equal([]byte{0, 0, 0, 0, 0, 0, 0, 0}))
		Expect(buf[8:16]).To(Equal([]byte{233, 3, 0, 0, 245, 1, 0, 0}))
		for _, c := range buf[16:] {
			Expect(c).To(Equal(uint8(0)))
		}
	})

	It("should support record callbacks", func() {
		subject.recWritten()
		subject.recWritten()
		subject.recDeleted()
		Expect(subject).To(Equal(&pageHeader{Version: 1, Stats: PageStats{1003, 502}}))
	})

})

var _ = Describe("Page", func() {
	var subject *Page

	BeforeEach(func() {
		var err error
		subject, err = openPage(filepath.Join(testDir, "00023.rcp"))
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		subject.close()
	})

	It("should open new files and write a header", func() {
		Expect(subject.id).To(Equal(uint32(23)))
		Expect(subject.offset).To(Equal(uint32(PAGE_HEADER_LEN)))
		Expect(subject.pos()).To(Equal(uint32(PAGE_HEADER_LEN)))
	})

	It("should reject invalid file names", func() {
		_, err := openPage(filepath.Join(testDir, "BAD"))
		Expect(err).To(Equal(ERROR_PAGE_INVALID))
	})

	It("should reopen files", func() {
		_, err := subject.append([]byte("key1"), []byte("some data"))
		Expect(err).NotTo(HaveOccurred())
		off, err := subject.append([]byte("key2"), []byte("more data"))
		Expect(err).NotTo(HaveOccurred())
		Expect(off).To(Equal(uint32(149)))
		subject.deleted()
		Expect(subject.header.Stats).To(Equal(PageStats{2, 1}))
		Expect(subject.close()).NotTo(HaveOccurred())

		subject, err = openPage(filepath.Join(testDir, "00023.rcp"))
		Expect(err).NotTo(HaveOccurred())
		Expect(subject.pos()).To(Equal(uint32(170)))
		Expect(subject.header.Stats).To(Equal(PageStats{2, 1}))
	})

	It("should fail to reopen corrupted file", func() {
		_, err := subject.file.WriteAt([]byte{'x'}, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(subject.close()).NotTo(HaveOccurred())

		_, err = openPage(filepath.Join(testDir, "00023.rcp"))
		Expect(err).To(Equal(ERROR_PAGE_BAD_HEADER))
	})

	It("should append/read data", func() {
		Expect(subject.pos()).To(Equal(uint32(PAGE_HEADER_LEN)))

		off, err := subject.append([]byte("key1"), []byte("data"))
		Expect(err).NotTo(HaveOccurred())
		Expect(off).To(Equal(uint32(PAGE_HEADER_LEN)))
		Expect(subject.pos()).To(Equal(uint32(144)))

		off, err = subject.append([]byte("key2"), []byte("more data"))
		Expect(off).To(Equal(uint32(144)))
		Expect(err).NotTo(HaveOccurred())

		Expect(subject.pos()).To(Equal(uint32(165)))
		Expect(subject.header.Stats).To(Equal(PageStats{2, 0}))

		raw := make([]byte, 16)
		_, err = subject.file.ReadAt(raw, 128)
		Expect(err).NotTo(HaveOccurred())
		Expect(raw).To(Equal([]byte{
			4, 0, // key length = 4
			4, 0, 0, 0, // val length = 4
			'k', 'e', 'y', '1', // key
			'd', 'a', 't', 'a', // value
			9, 189, // CRC-16
		}))

		key, value, err := subject.read(PAGE_HEADER_LEN)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(key)).To(Equal("key1"))
		Expect(string(value)).To(Equal("data"))

		key, value, err = subject.read(144)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(key)).To(Equal("key2"))
		Expect(string(value)).To(Equal("more data"))
	})

	It("should read known keys", func() {
		_, err := subject.append([]byte("key1"), []byte("data"))
		Expect(err).NotTo(HaveOccurred())

		_, err = subject.append([]byte("key2"), []byte("more data"))
		Expect(err).NotTo(HaveOccurred())

		value, err := subject.readKey([]byte("key2"), 144)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(value)).To(Equal("more data"))

		_, err = subject.readKey([]byte("key1"), 144)
		Expect(err).To(Equal(ERROR_INVALID_CHECKSUM))

		_, err = subject.readKey([]byte("key2"), 138)
		Expect(err).To(Equal(ERROR_INVALID_OFFSET))
	})

	It("should catch read/write errors", func() {
		_, _, err := subject.read(PAGE_HEADER_LEN)
		Expect(err).To(Equal(io.EOF))

		_, err = subject.append([]byte("key1"), []byte("data"))
		Expect(err).NotTo(HaveOccurred())

		_, _, err = subject.read(131)
		Expect(err).To(Equal(ERROR_INVALID_OFFSET))

		subject.file.WriteAt([]byte{'x'}, 138) // replace 'character'
		_, _, err = subject.read(PAGE_HEADER_LEN)
		Expect(err).To(Equal(ERROR_INVALID_CHECKSUM))
	})

	It("should parse pages", func() {
		_, err := subject.append([]byte("key1"), []byte("data"))
		Expect(err).NotTo(HaveOccurred())
		_, err = subject.append([]byte("key2"), []byte("more data"))
		Expect(err).NotTo(HaveOccurred())
		_, err = subject.append([]byte("key3"), []byte("even more data"))
		Expect(err).NotTo(HaveOccurred())

		kstore := NewHashKeyStore()
		Expect(subject.parse(kstore)).NotTo(HaveOccurred())
		Expect(kstore.refs).To(Equal(map[string]PageRef{
			"key1": {23, 128},
			"key2": {23, 144},
			"key3": {23, 165},
		}))
	})

	It("should delete entries", func() {
		subject.deleted()
		Expect(subject.header.Stats).To(Equal(PageStats{0, 1}))
	})

	It("should unlink pages", func() {
		err := subject.unlink()
		Expect(err).NotTo(HaveOccurred())

		_, err = os.Stat(filepath.Join(testDir, "00023.rcp"))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(`no such file or directory`))
	})

})
