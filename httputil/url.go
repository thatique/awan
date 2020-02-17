package httputil

// check if string url point to same site
// ie it relative file
func IsSameSiteURLPath(url string) bool {
	return len(url) >= 2 && url[0] == '/' && url[1] != '/' && url[1] != '\\'
}
