package rumcask

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Error", func() {

	It("should generate messages", func() {
		Expect(Error(-100).Error()).To(Equal("rumcask: database directory is locked by another process"))
		Expect(Error(12).Error()).To(Equal("rumcask: unknown error (12)"))
	})

})
