// +build openbsd netbsd freebsd dragonfly

package posix

import "syscall"

// True if dirent is absent in directory.
func isEmptyDirent(dirent *syscall.Dirent) bool {
	return dirent.Fileno == 0
}
