package lib

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
)

func run_s5cmd(in_path string, out_path string, wg *sync.WaitGroup) {
	// TODO: Fix this properly
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "DEBUG" {
		log.Println("Running s5cmd for", in_path)
	}
	// TODO: Check the order of defer statements, maybe we should close the file first and then do function defer using wg.Done()
	defer wg.Done()
	// Check if AWS_NO_SIGN_REQUEST environment variable is set to "YES"
	// If it is, we'll add the --no-sign-request flag to the s5cmd command
	args := []string{"--json", "ls", in_path}

	noSignRequest := os.Getenv("AWS_NO_SIGN_REQUEST")
	if noSignRequest == "YES" {
		args = append([]string{"--no-sign-request"}, args...)
	}

	requestPayer := os.Getenv("AWS_REQUEST_PAYER")
	if requestPayer == "requester" {
		args = append([]string{"--request-payer", "requester"}, args...)
	}

	cmd := exec.Command("s5cmd", args...)

	// open the out file for writing
	outfile, err := os.OpenFile(out_path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalln(err)
	}

	defer outfile.Close()
	cmd.Stdout = outfile

	err = cmd.Start()
	if err != nil {
		log.Fatalln(err)
	}
	cmd.Wait()
}

func read_s5cmd_output(fname string, deduplicate bool) ([]custom_file_path, error) {
	key_map := make(map[string]int)

	var file_paths []custom_file_path
	reader, err := os.Open(fname)
	check(err)

	decoder := json.NewDecoder(reader)
	for decoder.More() {
		var fp custom_file_path
		err := decoder.Decode(&fp)
		if err != nil {
			return nil, fmt.Errorf("parse fp: %w", err)
		}
		if deduplicate {
			_, ok := key_map[fp.Key]
			if !ok {
				file_paths = append(file_paths, fp)
				key_map[fp.Key] = 1
			}
		} else {
			file_paths = append(file_paths, fp)
		}
	}
	return file_paths, nil
}
