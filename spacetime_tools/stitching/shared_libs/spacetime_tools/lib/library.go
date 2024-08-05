package lib

import (
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

//export list_files_recursively
func list_files_recursively(in_paths []string, tmp_base_path string) []string {
	log.Println(tmp_base_path)
	// in_paths are in unix style, eg: /a/b/c.tif
	fmt.Println("Searching paths", len(in_paths))

	// Creates custom file objects so that we don't have to calculate parts and first_wildcard_idx everytime
	custom_in_paths := create_file_paths(in_paths)

	// Listing single level from s3 till first wildcard
	start := time.Now()
	var s3_wg sync.WaitGroup

	s3_wg.Add(len(custom_in_paths))

	for idx, path := range custom_in_paths {
		// TODO: Run s5cmd only for unique keys, right now we might be doing extra unnecessary work
		// Adding / at the end to list the files and folders at first level from that directory,
		// or else s3 returns the name of the parent folder
		// eg:
		// s3 ls s3://modis-pds/MCD43A4.006
		// s3 ls s3://modis-pds/MCD43A4.006/
		s := fmt.Sprintf("%s/", unix_to_s3(join_path_parts(path.parts[:path.first_wildcard_idx])))
		d := fmt.Sprintf("%s%d.json", tmp_base_path, idx)
		go run_s5cmd(s, d, &s3_wg)
	}
	s3_wg.Wait()
	log.Println("Time taken to get all the data from s3", time.Since(start))

	// Reading all the keys from results of s3
	start = time.Now()
	var matched_paths []custom_file_path
	for i := range custom_in_paths {
		s := fmt.Sprintf("%s%d.json", tmp_base_path, i)
		file_paths, err := read_s5cmd_output(s, false)
		check(err)

		// Converting to unix paths to match with the wildcard.
		// This is done because we first list all the files of that level and then filter out manually
		// As s3 when used with * returns recursively
		unix_file_paths, err := s3_to_unix(file_paths)
		check(err)

		// We iterate over all unix_file_paths and check if it matches with the regex
		for j := range unix_file_paths {
			var pat_till_first_wildcard string
			if (custom_in_paths[i].first_wildcard_idx + 1) == len(custom_in_paths[i].parts) {
				// Wildcard is the in the last part
				pat_till_first_wildcard = join_path_parts(custom_in_paths[i].parts[:custom_in_paths[i].first_wildcard_idx+1])
			} else {
				// Wildcard is in between file path
				pat_till_first_wildcard = join_path_parts(custom_in_paths[i].parts[:custom_in_paths[i].first_wildcard_idx+1]) + "/"
			}

			matched, err := path.Match(pat_till_first_wildcard, unix_file_paths[j].Key)
			check(err)

			if matched {
				parts_to_join := append(unix_file_paths[j].parts, custom_in_paths[i].parts[custom_in_paths[i].first_wildcard_idx+1:]...)
				matched_paths = append(matched_paths, create_file_paths([]string{join_path_parts(parts_to_join)})...)
			}
		}
	}

	log.Println("Time taken to run double loop", time.Since(start))
	log.Println("Matching paths found", len(matched_paths))

	var wildcard_patterns []string
	var file_list []string
	for _, mp := range matched_paths {
		if has_wildcard(mp) {
			wildcard_patterns = append(wildcard_patterns, mp.Key)
		} else {
			file_list = append(file_list, mp.Key)
		}
	}
	log.Println("Patterns with wildcards", len(wildcard_patterns))
	log.Println("File paths found", len(file_list))

	if len(wildcard_patterns) > 0 {
		return append(file_list, list_files_recursively(wildcard_patterns, tmp_base_path)...)
	} else {
		return file_list
	}
}

func ListFiles(in_paths []string, tmp_base_path string) []string {
	file_list := list_files_recursively(in_paths, tmp_base_path)
	return file_list
}

func SaveInventory(file_list []string, out_file string) {
	outfile, err := os.OpenFile(out_file, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer outfile.Close()

	str := strings.Join(file_list, "\n")

	outfile.WriteString(str)
}
