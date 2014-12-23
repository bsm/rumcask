package rumcask

import (
	"path/filepath"

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

	It("should get records", func() {
		_, err := subject.Get([]byte("key1"))
		Expect(err).To(Equal(NOT_FOUND))

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
		Expect(err).To(Equal(NOT_FOUND))
	})

	It("should reopen DBs", func() {
		fill()
		Expect(subject.pages).To(HaveLen(2))

		err := subject.Close()
		Expect(err).NotTo(HaveOccurred())

		subject, err = Open(testDir, keys)
		Expect(err).NotTo(HaveOccurred())

		Expect(subject.pages).To(HaveLen(2))
		Expect(subject.pages[0].header.Stats).To(Equal(PageStats{3, 1}))
		Expect(subject.pages[1].header.Stats).To(Equal(PageStats{3, 0}))

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

})
