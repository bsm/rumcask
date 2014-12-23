package rumcask

import (
	"io/ioutil"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

/** Test hook **/

var testDir string

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	BeforeEach(func() {
		var err error
		testDir, err = ioutil.TempDir("", "rumcask-tests")
		Expect(err).NotTo(HaveOccurred())
	})
	AfterEach(func() {
		os.RemoveAll(testDir)
	})
	RunSpecs(t, "rumcask")
}

type writeAtBuffer struct{ b []byte }

func (w *writeAtBuffer) WriteAt(p []byte, off int64) (int, error) {
	copy(w.b[int(off):], p)
	return len(p), nil
}
