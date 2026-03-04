// Package parse provides shared SQL string-parsing utilities used by the ir
// and sql conversion layers. All functions are pure (no I/O, no global state).
package parse

import (
	"errors"
	"strings"
)

// IsIdentChar reports whether c is a valid SQL identifier character (A-Z, 0-9, _ or .).
func IsIdentChar(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.'
}

// HasKeywordAtWordBoundary reports whether upper[i:] starts with kw at a word boundary.
// upper must already be upper-cased.
func HasKeywordAtWordBoundary(upper string, i int, kw string) bool {
	if i < 0 || i+len(kw) > len(upper) {
		return false
	}
	if upper[i:i+len(kw)] != kw {
		return false
	}
	if i > 0 {
		c := upper[i-1]
		if (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			return false
		}
	}
	if i+len(kw) < len(upper) {
		c := upper[i+len(kw)]
		if (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			return false
		}
	}
	return true
}

// IndexKeyword finds the first occurrence of kw as a whole word at paren depth 0.
// upperSQL must already be upper-cased.
// Returns -1 if not found.
func IndexKeyword(upperSQL, kw string) int {
	depth := 0
	for i := 0; i < len(upperSQL); i++ {
		switch upperSQL[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 {
			continue
		}
		if i+len(kw) <= len(upperSQL) && upperSQL[i:i+len(kw)] == kw {
			leftOK := i == 0 || !IsIdentChar(upperSQL[i-1])
			rightOK := i+len(kw) == len(upperSQL) || !IsIdentChar(upperSQL[i+len(kw)])
			if leftOK && rightOK {
				return i
			}
		}
	}
	return -1
}

// ContainsKeywordOutsideParens reports whether keyword (space-padded) appears
// at paren depth 0 in s.
func ContainsKeywordOutsideParens(s, keyword string) bool {
	upper := strings.ToUpper(s)
	kw := " " + strings.ToUpper(keyword) + " "
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 && i+len(kw) <= len(upper) && upper[i:i+len(kw)] == kw {
			return true
		}
	}
	return false
}

// SplitByKeyword splits s by keyword (space-padded) only at paren depth 0.
func SplitByKeyword(s, keyword string) []string {
	upper := strings.ToUpper(s)
	kw := " " + strings.ToUpper(keyword) + " "
	var parts []string
	depth, lastIdx := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 && i+len(kw) <= len(upper) && upper[i:i+len(kw)] == kw {
			parts = append(parts, strings.TrimSpace(s[lastIdx:i]))
			lastIdx = i + len(kw)
			i = lastIdx - 1
		}
	}
	if lastIdx < len(s) {
		parts = append(parts, strings.TrimSpace(s[lastIdx:]))
	}
	return parts
}

// IsBalancedAndOuter reports whether s is enclosed in a single outer paren pair.
func IsBalancedAndOuter(s string) bool {
	if !strings.HasPrefix(s, "(") || !strings.HasSuffix(s, ")") {
		return false
	}
	depth := 0
	for i, ch := range s {
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
			if depth == 0 && i < len(s)-1 {
				return false
			}
		}
	}
	return depth == 0
}

// SplitByComma splits s by ',' at paren depth 0.
func SplitByComma(s string) []string {
	var parts []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	if start < len(s) {
		parts = append(parts, s[start:])
	}
	return parts
}

// FindMatchingParen finds the index of the ')' matching '(' at open.
// Returns -1 and an error if not found.
func FindMatchingParen(expr string, open int) (int, error) {
	if open < 0 || open >= len(expr) || expr[open] != '(' {
		return -1, errors.New("malformed function call")
	}
	depth := 0
	for i := open; i < len(expr); i++ {
		switch expr[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}
	return -1, errors.New("malformed function call")
}
