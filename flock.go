package rumcask

// Adapted from https://github.com/syndtr/goleveldb
//
// Original copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// https://github.com/syndtr/goleveldb/blob/b02b57/LICENSE.

import (
	"os"
	"syscall"
)

type fileLock struct {
	f *os.File
}

func newFileLock(fname string) (fl *fileLock, err error) {
	fl = &fileLock{}
	if fl.f, err = os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		fl = nil
		return
	}

	if err = fl.flock(syscall.LOCK_EX); err != nil {
		if err == syscall.EAGAIN {
			err = ERROR_DB_LOCKED
		}
		fl.f.Close()
		fl = nil
	}
	return
}

func (fl *fileLock) release() error {
	if err := fl.flock(syscall.LOCK_UN); err != nil {
		return err
	}
	return fl.f.Close()
}

func (fl *fileLock) flock(flag int) error {
	return syscall.Flock(int(fl.f.Fd()), flag|syscall.LOCK_NB)
}
