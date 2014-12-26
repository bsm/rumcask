package rumcask

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("DB", func() {
	var subject *DB
	var keys *HashKeyStore
	var fill = func() {
		ok, err := subject.Set([]byte("key1"), []byte("val1"))
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())
		ok, err = subject.Set([]byte("key2"), []byte("val2"))
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())
		ok, err = subject.Set([]byte("key3"), []byte("val3"))
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())

		Expect(subject.nextPage()).NotTo(HaveOccurred())
		ok, err = subject.Set([]byte("key4"), []byte("val4"))
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())
		ok, err = subject.Set([]byte("key2"), []byte("valX"))
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		ok, err = subject.Set([]byte("key5"), []byte("val5"))
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())
	}

	BeforeEach(func() {
		var err error
		keys = NewHashKeyStore()
		subject, err = Open(testDir, keys)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		subject.Close()
	})

	It("should create locks", func() {
		items, _ := filepath.Glob(filepath.Join(testDir, "*"))
		Expect(items).To(ConsistOf([]string{
			filepath.Join(testDir, "00000000.rcp"),
			filepath.Join(testDir, "LOCK"),
		}))
	})

	It("should create/link pages", func() {
		Expect(subject.pages).To(HaveLen(1))
		Expect(subject.pages).To(HaveKeyWithValue(uint32(0), subject.current))

		Expect(subject.current).NotTo(BeNil())
		Expect(subject.current.id).To(Equal(uint32(0)))
		Expect(subject.current.offset).To(Equal(uint32(PAGE_HEADER_LEN)))
	})

	It("should add records and rotate pages", func() {
		fill()
		Expect(subject.pages).To(HaveLen(2))
		Expect(subject.pages).To(HaveKeyWithValue(uint32(1), subject.current))

		Expect(subject.current).NotTo(BeNil())
		Expect(subject.current.id).To(Equal(uint32(1)))

		Expect(keys.refs).To(Equal(map[string]PageRef{
			"key1": {ID: 0, Offset: 128},
			"key2": {ID: 1, Offset: 144},
			"key3": {ID: 0, Offset: 160},
			"key4": {ID: 1, Offset: 128},
			"key5": {ID: 1, Offset: 160},
		}))
	})

	It("should validate arguments", func() {
		_, err := subject.Set([]byte(""), []byte("val1"))
		Expect(err).To(Equal(ERROR_KEY_BLANK))
		_, err = subject.Set(nil, []byte("val1"))
		Expect(err).To(Equal(ERROR_KEY_BLANK))
		_, err = subject.Set([]byte("key1"), nil)
		Expect(err).To(Equal(ERROR_VALUE_BLANK))
		_, err = subject.Set([]byte("key1"), []byte{})
		Expect(err).To(Equal(ERROR_VALUE_BLANK))
		_, err = subject.Set(bytes.Repeat([]byte{'k'}, MAX_KEY_LEN+1), []byte("val1"))
		Expect(err).To(Equal(ERROR_KEY_TOO_LONG))
	})

	It("should get records", func() {
		_, err := subject.Get([]byte("key1"))
		Expect(err).To(Equal(ERROR_NOT_FOUND))

		fill()
		val, err := subject.Get([]byte("key1"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("val1")))
		val, err = subject.Get([]byte("key2"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("valX")))
		val, err = subject.Get([]byte("key3"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("val3")))
		_, err = subject.Get([]byte("key9"))
		Expect(err).To(Equal(ERROR_NOT_FOUND))
	})

	It("should delete records", func() {
		ok, err := subject.Delete([]byte("key3"))
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeFalse())

		fill()
		Expect(subject.current.offset).To(Equal(uint32(176)))

		ok, err = subject.Delete([]byte("key3"))
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())

		Expect(subject.pages).To(HaveLen(2))
		Expect(subject.pages[0].header.Stats).To(Equal(PageStats{3, 2}))
		Expect(subject.pages[1].header.Stats).To(Equal(PageStats{3, 0}))
		Expect(keys.refs).To(Equal(map[string]PageRef{
			"key1": {ID: 0, Offset: 128},
			"key2": {ID: 1, Offset: 144},
			"key4": {ID: 1, Offset: 128},
			"key5": {ID: 1, Offset: 160},
		}))

		_, err = subject.Get([]byte("key3"))
		Expect(err).To(Equal(ERROR_NOT_FOUND))
	})

	It("should reopen DBs", func() {
		fill()
		Expect(subject.pages).To(HaveLen(2))
		_, err := subject.Delete([]byte("key3"))
		Expect(err).NotTo(HaveOccurred())
		Expect(subject.Close()).NotTo(HaveOccurred())

		subject, err = Open(testDir, keys)
		Expect(err).NotTo(HaveOccurred())

		Expect(subject.pages).To(HaveLen(2))
		Expect(subject.pages[0].header.Stats).To(Equal(PageStats{3, 2}))
		Expect(subject.pages[1].header.Stats).To(Equal(PageStats{3, 0}))

		Expect(subject.current).NotTo(BeNil())
		Expect(subject.current.id).To(Equal(uint32(1)))
		Expect(keys.refs).To(Equal(map[string]PageRef{
			"key1": {ID: 0, Offset: 128},
			"key2": {ID: 1, Offset: 144},
			"key4": {ID: 1, Offset: 128},
			"key5": {ID: 1, Offset: 160},
		}))
	})

})

func BenchmarkDB_Writes_64(b *testing.B) { benchDB_writes(b, 64) }
func BenchmarkDB_Writes_1K(b *testing.B) { benchDB_writes(b, 1*KiB) }
func BenchmarkDB_Writes_1M(b *testing.B) { benchDB_writes(b, 1*MiB) }
func BenchmarkDB_Reads_64(b *testing.B)  { benchDB_reads(b, 64) }
func BenchmarkDB_Reads_1K(b *testing.B)  { benchDB_reads(b, 1*KiB) }
func BenchmarkDB_Reads_1M(b *testing.B)  { benchDB_reads(b, 1*MiB) }

func benchDB_run(b *testing.B, call func(*DB) error) {
	dir, err := ioutil.TempDir("", "rumcask-bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir, NewHashKeyStore())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	if err := call(db); err != nil {
		b.Fatal(err)
	}
}

func benchDB_reads(b *testing.B, size int) {
	value := bytes.Repeat([]byte{'X'}, size)
	benchDB_run(b, func(db *DB) error {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("KEY%08d", i)
			if _, err := db.Set([]byte(key), value); err != nil {
				return err
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("KEY%08d", rand.Intn(b.N))
			if _, err := db.Get([]byte(key)); err != nil {
				return err
			}
		}
		return nil
	})

}

func benchDB_writes(b *testing.B, size int) {
	value := bytes.Repeat([]byte{'X'}, size)
	benchDB_run(b, func(db *DB) error {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("KEY%08d", rand.Intn(b.N))
			_, err := db.Set([]byte(key), value)
			if err != nil {
				return err
			}
		}
		return nil
	})

}
