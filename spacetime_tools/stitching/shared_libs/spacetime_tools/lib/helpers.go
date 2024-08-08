package lib

import (
	"strings"
)

func s3_to_unix(s3_paths []custom_file_path) ([]custom_file_path, error) {
	var unix_paths []string
	for _, fp := range s3_paths {
		up := strings.Replace(fp.Key, "s3://", "/", -1)
		unix_paths = append(unix_paths, up)
	}

	return create_file_paths(unix_paths), nil
}

func unix_to_s3(path string) string {
	if path[0] == '/' {
		return "s3://" + path[1:]
	}

	return path
}

func get_path_parts(file_path string) []string {
	return strings.Split(file_path, "/")
}

func get_first_wildcard_idx(path_parts []string) int {
	for idx, part := range path_parts {
		if strings.Contains(part, "*") || strings.Contains(part, "?") {
			return idx
		}
	}
	return -1
}

func has_wildcard(fp custom_file_path) bool {
	if strings.Contains(fp.Key, "*") || strings.Contains(fp.Key, "?") {
		return true
	}
	return false
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func remove_empty(parts []string) []string {
	var r []string
	for _, p := range parts {
		if p != "" {
			r = append(r, p)
		}
	}
	return r
}
func join_path_parts(parts []string) string {
	return "/" + strings.Join(remove_empty(parts), "/")
}
