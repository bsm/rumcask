package rumcask

import (
	"path/filepath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("fileLock", func() {

	It("should lock files exclusively", func() {
		fname := filepath.Join(testDir, "LOCK")
		flock, err := newFileLock(fname)
		Expect(err).NotTo(HaveOccurred())
		defer flock.release()

		_, err = newFileLock(fname)
		Expect(err).To(Equal(ERROR_DB_LOCKED))
	})

})
