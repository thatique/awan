package posix

import (
	"errors"
	"os"
	"runtime"
	"syscall"
)

var (
	// ErrFileNotFound - cannot find the file.
	ErrFileNotFound = errors.New("file not found")

	// ErrFileAccessDenied - cannot access file, insufficient permissions.
	ErrFileAccessDenied = errors.New("file access denied")
)

// IsSysErrNoSys check if given error is Function not implemented error
func IsSysErrNoSys(err error) bool {
	if err == syscall.ENOSYS {
		return true
	}
	pathErr, ok := err.(*os.PathError)
	return ok && pathErr.Err == syscall.ENOSYS
}

// IsSysErrOpNotSupported Not supported error
func IsSysErrOpNotSupported(err error) bool {
	if err == syscall.EOPNOTSUPP {
		return true
	}
	pathErr, ok := err.(*os.PathError)
	return ok && pathErr.Err == syscall.EOPNOTSUPP

}

// IsSysErrNoSpace No space left on device error
func IsSysErrNoSpace(err error) bool {
	if err == syscall.ENOSPC {
		return true
	}
	pathErr, ok := err.(*os.PathError)
	return ok && pathErr.Err == syscall.ENOSPC
}

// IsSysErrIO check Input/output error
func IsSysErrIO(err error) bool {
	if err == syscall.EIO {
		return true
	}
	pathErr, ok := err.(*os.PathError)
	return ok && pathErr.Err == syscall.EIO
}

// IsSysErrIsDir Check if the given error corresponds to EISDIR (is a directory).
func IsSysErrIsDir(err error) bool {
	if err == syscall.EISDIR {
		return true
	}
	pathErr, ok := err.(*os.PathError)
	return ok && pathErr.Err == syscall.EISDIR

}

// IsSysErrNotDir Check if the given error corresponds to ENOTDIR (is not a directory).
func IsSysErrNotDir(err error) bool {
	if err == syscall.ENOTDIR {
		return true
	}
	pathErr, ok := err.(*os.PathError)
	return ok && pathErr.Err == syscall.ENOTDIR
}

// IsSysErrTooLong Check if the given error corresponds to the ENAMETOOLONG (name too long).
func IsSysErrTooLong(err error) bool {
	if err == syscall.ENAMETOOLONG {
		return true
	}
	pathErr, ok := err.(*os.PathError)
	return ok && pathErr.Err == syscall.ENAMETOOLONG
}

// IsSysErrNotEmpty Check if the given error corresponds to ENOTEMPTY for unix
// and ERROR_DIR_NOT_EMPTY for windows (directory not empty).
func IsSysErrNotEmpty(err error) bool {
	if err == syscall.ENOTEMPTY {
		return true
	}
	if pathErr, ok := err.(*os.PathError); ok {
		if runtime.GOOS == "windows" {
			if errno, _ok := pathErr.Err.(syscall.Errno); _ok && errno == 0x91 {
				// ERROR_DIR_NOT_EMPTY
				return true
			}
		}
		if pathErr.Err == syscall.ENOTEMPTY {
			return true
		}
	}
	return false
}

// IsSysErrPathNotFound Check if the given error corresponds to the specific ERROR_PATH_NOT_FOUND for windows
func IsSysErrPathNotFound(err error) bool {
	if runtime.GOOS != "windows" {
		return false
	}
	if pathErr, ok := err.(*os.PathError); ok {
		if errno, _ok := pathErr.Err.(syscall.Errno); _ok && errno == 0x03 {
			// ERROR_PATH_NOT_FOUND
			return true
		}
	}
	return false
}

// IsSysErrHandleInvalid Check if the given error corresponds to the specific ERROR_INVALID_HANDLE for windows
func IsSysErrHandleInvalid(err error) bool {
	if runtime.GOOS != "windows" {
		return false
	}
	// Check if err contains ERROR_INVALID_HANDLE errno
	errno, ok := err.(syscall.Errno)
	return ok && errno == 0x6
}

// IsSysErrCrossDevice check the given error is os.LinkError
func IsSysErrCrossDevice(err error) bool {
	e, ok := err.(*os.LinkError)
	return ok && e.Err == syscall.EXDEV
}
