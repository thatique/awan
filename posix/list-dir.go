package posix

// ReadDir return all the entries at the directory dirPath.
func ReadDir(dirPath string) (entries []string, err error) {
	return ReadDirN(dirPath, -1)
}

// ReadDirN return N entries at the directory dirPath. If count is -1, return all entries
func ReadDirN(dirPath string, count int) (entries []string, err error) {
	return readDirN(dirPath, count)
}
