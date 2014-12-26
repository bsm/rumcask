package btree

import (
	"testing"

	"github.com/bsm/rumcask"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("KeyStore", func() {
	var subject *KeyStore
	var _ rumcask.KeyStore = subject // interface assertions

	BeforeEach(func() {
		subject = NewKeyStore(3)
	})

	It("should store/fetch/delete", func() {
		_, ok := subject.Fetch([]byte("key1"))
		Expect(ok).To(BeFalse())

		_, ok = subject.Store([]byte("key1"), rumcask.PageRef{1, 1024})
		Expect(ok).To(BeFalse())
		_, ok = subject.Store([]byte("key2"), rumcask.PageRef{7, 8096})
		Expect(ok).To(BeFalse())

		ref, ok := subject.Store([]byte("key1"), rumcask.PageRef{2, 2048})
		Expect(ok).To(BeTrue())
		Expect(ref).To(Equal(rumcask.PageRef{1, 1024}))

		ref, ok = subject.Fetch([]byte("key1"))
		Expect(ok).To(BeTrue())
		Expect(ref).To(Equal(rumcask.PageRef{2, 2048}))

		ref, ok = subject.Delete([]byte("key2"))
		Expect(ok).To(BeTrue())
		Expect(ref).To(Equal(rumcask.PageRef{7, 8096}))
		_, ok = subject.Delete([]byte("key2"))
		Expect(ok).To(BeFalse())
	})

	It("should have a len", func() {
		_, ok := subject.Store([]byte("key1"), rumcask.PageRef{1, 1024})
		Expect(ok).To(BeFalse())
		Expect(subject.Len()).To(Equal(1))

		_, ok = subject.Store([]byte("key2"), rumcask.PageRef{7, 8096})
		Expect(ok).To(BeFalse())
		Expect(subject.Len()).To(Equal(2))
	})

})

/** Test hook **/

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "rumcask/btree")
}
