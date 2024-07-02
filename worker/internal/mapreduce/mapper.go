package mapreduce

import (
	"mapreduce_worker/internal/dto"
	"strings"
	"unicode"
)

func Map(filename string, contents string) []dto.KeyValue {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	words := strings.FieldsFunc(contents, ff)

	kva := []dto.KeyValue{}
	for _, w := range words {
		kv := dto.KeyValue{Key: w, Value: "1"}
		kva = append(kva, kv)
	}
	return kva
}
