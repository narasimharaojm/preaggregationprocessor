// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package preaggregationprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor"

import (
	"regexp"
	"strings"
)

// globToRegex converts a glob pattern to a regular expression pattern.
// Glob syntax:
//   - * matches any characters (including none)
//   - ? matches exactly one character
//   - All other characters are treated as literals
//
// Examples:
//   - "*.min" -> "^.*\.min$"
//   - "cpu.*" -> "^cpu\..*$"
//   - "test.?.value" -> "^test\..\.value$"
//   - "*usage*" -> "^.*usage.*$"
func globToRegex(pattern string) string {
	var result strings.Builder

	// Start anchor
	result.WriteString("^")

	// Convert glob pattern to regex
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		switch ch {
		case '*':
			// * in glob matches zero or more of any character
			result.WriteString(".*")
		case '?':
			// ? in glob matches exactly one character
			result.WriteString(".")
		case '.', '+', '(', ')', '|', '[', ']', '{', '}', '^', '$', '\\':
			// Escape regex special characters
			result.WriteString("\\")
			result.WriteByte(ch)
		default:
			// Regular character
			result.WriteByte(ch)
		}
	}

	// End anchor
	result.WriteString("$")

	return result.String()
}

// convertGlobPatternsToRegex converts a slice of glob patterns to regex patterns
func convertGlobPatternsToRegex(globPatterns []string) ([]string, error) {
	if len(globPatterns) == 0 {
		return globPatterns, nil
	}

	regexPatterns := make([]string, len(globPatterns))
	for i, globPattern := range globPatterns {
		regexPattern := globToRegex(globPattern)

		// Validate the generated regex
		_, err := regexp.Compile(regexPattern)
		if err != nil {
			return nil, err
		}

		regexPatterns[i] = regexPattern
	}

	return regexPatterns, nil
}
