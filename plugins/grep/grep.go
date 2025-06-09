package main

import (
	"go-mapreduce/mapreduce"
	"os"
	"regexp"
	"strings"
)

// grepPatternEnv is the env var used to specify the grep pattern.
const grepPatternEnv = "GREP_PATTERN"

// re holds the compiled regex pattern used to match lines.
var re *regexp.Regexp

// init compiles the regex from GREP_PATTERN or falls back to "A".
func init() {
	pattern := os.Getenv(grepPatternEnv)
	if pattern == "" {
		pattern = "A"
	}
	var err error
	re, err = regexp.Compile(pattern)
	if err != nil {
		// fallback to literal match for invalid regex
		re = regexp.MustCompile(regexp.QuoteMeta(pattern))
	}
}

// Map emits a KV pair for every line matching the regex.
// The key is the filename and the value is the matching line.
func Map(filename, contents string) []mapreduce.KVPair {
	lines := strings.Split(contents, "\n")
	pairs := []mapreduce.KVPair{}
	for _, line := range lines {
		if re.MatchString(line) {
			pairs = append(pairs, mapreduce.KVPair{Key: filename, Value: line})
		}
	}
	return pairs
}

// Reduce concatenates all matching lines for a given file.
func Reduce(key string, values []string) string {
	return strings.Join(values, "\n")
}
