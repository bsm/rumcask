package rumcask

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CRC", func() {

	It("should calculate CRC-16 digests", func() {
		Expect(CRC16([]byte("123456789"))).To(Equal(uint16(12739)))
	})

})
