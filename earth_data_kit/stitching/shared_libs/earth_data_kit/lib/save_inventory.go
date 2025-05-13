package lib

import (
	"log"
	"os"
	"strings"
)

func SaveInventory(file_list []string, out_file string) {
	outfile, err := os.OpenFile(out_file, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalln(err)
	}
	defer outfile.Close()

	str := strings.Join(file_list, "\n")

	outfile.WriteString(str)
}
