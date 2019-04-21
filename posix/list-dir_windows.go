// +build windows

package posix

import (
	"errors"
	"os"
	"path"
	"strings"
	"syscall"
)

// ReadDirN return N entries at the directory dirPath. If count is -1, return all entries
func readDirN(dirpath string, count int) (entries []string, err error) {
	d, err := os.Open(dirPath)
	if err != nil {
		// File is really not found.
		if os.IsNotExist(err) {
			return nil, ErrFileNotFound
		}

		// File path cannot be verified since one of the parents is a file.
		if strings.Contains(err.Error(), "not a directory") {
			return nil, ErrFileNotFound
		}
		return nil, err
	}
	defer d.Close()

	st, err := d.Stat()
	if err != nil {
		return nil, err
	}
	// Not a directory return error.
	if !st.IsDir() {
		return nil, errors.New("file access denied")
	}

	data := &syscall.Win32finddata{}

	remaining := count
	done := false
	for !done {
		e := syscall.FindNextFile(syscall.Handle(d.Fd()), data)
		if e != nil {
			if e == syscall.ERROR_NO_MORE_FILES {
				break
			} else {
				err = &os.PathError{
					Op:   "FindNextFile",
					Path: dirPath,
					Err:  e,
				}
				return
			}
		}
		name := syscall.UTF16ToString(data.FileName[0:])
		if name == "." || name == ".." { // Useless names
			continue
		}
		switch {
		case data.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0:
			// If its symbolic link, follow the link using os.Stat()
			var fi os.FileInfo
			fi, err = os.Stat(path.Join(dirPath, name))
			if err != nil {
				// If file does not exist, we continue and skip it.
				// Could happen if it was deleted in the middle while
				// this list was being performed.
				if os.IsNotExist(err) {
					continue
				}
				return nil, err
			}
			if fi.IsDir() {
				entries = append(entries, name+slashSeparator)
			} else if fi.Mode().IsRegular() {
				entries = append(entries, name)
			}
		case data.FileAttributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0:
			entries = append(entries, name+slashSeparator)
		default:
			entries = append(entries, name)
		}
		if remaining > 0 {
			remaining--
			done = remaining == 0
		}
	}
	return entries, nil
}
