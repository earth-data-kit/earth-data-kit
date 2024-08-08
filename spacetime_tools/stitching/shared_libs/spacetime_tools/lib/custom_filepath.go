package lib

type custom_file_path struct {
	Key                string `json:"key"`
	parts              []string
	first_wildcard_idx int
}

func create_file_paths(paths []string) []custom_file_path {
	var objs []custom_file_path

	for i := range paths {
		parts := get_path_parts(paths[i])
		// Adds all the parts, index till first wildcard and key in the object
		obj := custom_file_path{Key: paths[i], parts: parts, first_wildcard_idx: get_first_wildcard_idx(parts)}
		objs = append(objs, obj)
	}

	return objs
}
