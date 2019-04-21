// +build darwin linux

package posix

import "syscall"

// isEmptyDirent True if dirent is absent in directory.
func isEmptyDirent(dirent *syscall.Dirent) bool {
	return dirent.Ino == 0
}
