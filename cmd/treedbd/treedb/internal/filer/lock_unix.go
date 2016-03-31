// +build !windows

package filer

import (
	"os"
	"syscall"
)

func LockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func UnlockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN|syscall.LOCK_NB)
}
