package rumcask

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("HashKeyStore", func() {
	var subject *HashKeyStore
	var _ KeyStore = subject // interface assertions

	BeforeEach(func() {
		subject = NewHashKeyStore()
	})

	It("should store/fetch/delete", func() {
		_, ok := subject.Fetch([]byte("key1"))
		Expect(ok).To(BeFalse())

		_, ok = subject.Store([]byte("key1"), PageRef{1, 1024})
		Expect(ok).To(BeFalse())
		_, ok = subject.Store([]byte("key2"), PageRef{7, 8096})
		Expect(ok).To(BeFalse())

		ref, ok := subject.Store([]byte("key1"), PageRef{2, 2048})
		Expect(ok).To(BeTrue())
		Expect(ref).To(Equal(PageRef{1, 1024}))

		ref, ok = subject.Fetch([]byte("key1"))
		Expect(ok).To(BeTrue())
		Expect(ref).To(Equal(PageRef{2, 2048}))

		ref, ok = subject.Delete([]byte("key2"))
		Expect(ok).To(BeTrue())
		Expect(ref).To(Equal(PageRef{7, 8096}))
		_, ok = subject.Delete([]byte("key2"))
		Expect(ok).To(BeFalse())
	})

})
