package main

import (
	"bufio"
	"earth-data-kit/lib"
	"log"
	"os"
)

func main() {
	// Clearing the tmp directory for s3
	tmp_base_path := os.Getenv("TMP_DIR") + "/s3/"
	os.RemoveAll(tmp_base_path)
	os.MkdirAll(tmp_base_path, 0777)

	args := os.Args[1:]

	input_file := args[0]
	output_file := args[1]
	file, err := os.OpenFile(input_file, os.O_RDONLY, 0777)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	var lines []string
	sc := bufio.NewScanner(file)
	for sc.Scan() {
		line := sc.Text()
		lines = append(lines, line)
	}

	file_list := lib.ListFiles(lines, tmp_base_path)

	lib.SaveInventory(file_list, output_file)
	os.RemoveAll(tmp_base_path)
}
