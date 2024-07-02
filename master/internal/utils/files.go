package utils

import (
	"fmt"
	"strings"
)

// file has the extension .txt
// output file has no extension
func GenerateIntermediateFile(file string) string {
	return strings.Split(file, ".txt")[0] + "-temp.txt"
}

func GenerateOutputPartFile(file string, index int) string {
	return fmt.Sprintf("%s-%d", file, index)
}
