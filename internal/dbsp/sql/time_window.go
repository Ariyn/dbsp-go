package sqlconv

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/ariyn/dbsp/internal/dbsp/ir"
	"github.com/ariyn/dbsp/internal/dbsp/types"
)

// ParseTimeWindowSQL parses time window SQL functions like:
// - TUMBLE(ts, INTERVAL '5' MINUTE)
// - HOP(ts, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE)  -- (size, slide)
// - SESSION(ts, INTERVAL '5' MINUTE)  -- (gap)
func ParseTimeWindowSQL(windowSQL string) (*ir.TimeWindowSpec, error) {
	windowSQL = strings.TrimSpace(windowSQL)
	windowSQL = strings.Trim(windowSQL, "'\"")

	// Extract function name and arguments
	funcPattern := regexp.MustCompile(`^(\w+)\s*\((.*)\)$`)
	matches := funcPattern.FindStringSubmatch(windowSQL)
	if len(matches) != 3 {
		return nil, fmt.Errorf("invalid window function syntax: %s", windowSQL)
	}

	funcName := strings.ToUpper(matches[1])
	argsStr := matches[2]

	// Split arguments by comma (being careful with nested parentheses)
	args := splitWindowArgs(argsStr)

	switch funcName {
	case "TUMBLE":
		return parseTumbleWindow(args)
	case "HOP", "SLIDING":
		return parseHopWindow(args)
	case "SESSION":
		return parseSessionWindow(args)
	default:
		return nil, fmt.Errorf("unsupported time window function: %s", funcName)
	}
}

// parseTumbleWindow parses TUMBLE(time_col, size)
func parseTumbleWindow(args []string) (*ir.TimeWindowSpec, error) {
	if len(args) != 2 {
		return nil, errors.New("TUMBLE requires 2 arguments: time_column, size")
	}

	timeCol := strings.TrimSpace(args[0])
	sizeMillis, err := parseIntervalArg(args[1])
	if err != nil {
		return nil, fmt.Errorf("invalid TUMBLE size: %v", err)
	}

	return &ir.TimeWindowSpec{
		WindowType:  "TUMBLING",
		TimeCol:     timeCol,
		SizeMillis:  sizeMillis,
		SlideMillis: 0,
		GapMillis:   0,
	}, nil
}

// parseHopWindow parses HOP(time_col, slide, size)
func parseHopWindow(args []string) (*ir.TimeWindowSpec, error) {
	if len(args) != 3 {
		return nil, errors.New("HOP requires 3 arguments: time_column, slide, size")
	}

	timeCol := strings.TrimSpace(args[0])
	slideMillis, err := parseIntervalArg(args[1])
	if err != nil {
		return nil, fmt.Errorf("invalid HOP slide: %v", err)
	}
	sizeMillis, err := parseIntervalArg(args[2])
	if err != nil {
		return nil, fmt.Errorf("invalid HOP size: %v", err)
	}

	return &ir.TimeWindowSpec{
		WindowType:  "SLIDING",
		TimeCol:     timeCol,
		SizeMillis:  sizeMillis,
		SlideMillis: slideMillis,
		GapMillis:   0,
	}, nil
}

// parseSessionWindow parses SESSION(time_col, gap)
func parseSessionWindow(args []string) (*ir.TimeWindowSpec, error) {
	if len(args) != 2 {
		return nil, errors.New("SESSION requires 2 arguments: time_column, gap")
	}

	timeCol := strings.TrimSpace(args[0])
	gapMillis, err := parseIntervalArg(args[1])
	if err != nil {
		return nil, fmt.Errorf("invalid SESSION gap: %v", err)
	}

	return &ir.TimeWindowSpec{
		WindowType:  "SESSION",
		TimeCol:     timeCol,
		SizeMillis:  0,
		SlideMillis: 0,
		GapMillis:   gapMillis,
	}, nil
}

// parseIntervalArg parses INTERVAL expressions or uses types.ParseInterval
func parseIntervalArg(arg string) (int64, error) {
	arg = strings.TrimSpace(arg)
	arg = strings.Trim(arg, "'\"") // Remove quotes

	// Try parsing as INTERVAL 'N' UNIT or INTERVAL N UNIT
	if strings.HasPrefix(strings.ToUpper(arg), "INTERVAL") {
		// Remove "INTERVAL" prefix
		rest := strings.TrimSpace(arg[8:])
		rest = strings.Trim(rest, "'\"") // Remove any remaining quotes
		
		interval, err := types.ParseInterval(rest)
		if err != nil {
			return 0, err
		}
		return interval.Millis, nil
	}

	// Try parsing as plain duration string
	interval, err := types.ParseInterval(arg)
	if err != nil {
		return 0, err
	}
	return interval.Millis, nil
}

// splitWindowArgs splits function arguments by comma, respecting nested parentheses and quotes
func splitWindowArgs(argsStr string) []string {
	var args []string
	var current strings.Builder
	parenDepth := 0
	quoteChar := rune(0)

	for _, ch := range argsStr {
		switch {
		case quoteChar != 0:
			// Inside quotes - don't add quote chars, just content
			if ch == quoteChar {
				quoteChar = 0 // End of quote
			} else {
				current.WriteRune(ch) // Add content without quotes
			}
		case ch == '\'' || ch == '"':
			// Start of quote - don't add the quote char
			quoteChar = ch
		case ch == '(':
			parenDepth++
			current.WriteRune(ch)
		case ch == ')':
			parenDepth--
			current.WriteRune(ch)
		case ch == ',' && parenDepth == 0:
			// Argument separator at top level
			args = append(args, strings.TrimSpace(current.String()))
			current.Reset()
		default:
			current.WriteRune(ch)
		}
	}

	// Add last argument
	if current.Len() > 0 {
		args = append(args, strings.TrimSpace(current.String()))
	}

	return args
}

// ParseTimeWindowFromGroupBy extracts time window specification from GROUP BY clause
// Looks for patterns like: GROUP BY TUMBLE(ts, INTERVAL '5' MINUTE)
func ParseTimeWindowFromGroupBy(groupByExprs []string) (*ir.TimeWindowSpec, []string, error) {
	for i, expr := range groupByExprs {
		expr = strings.TrimSpace(expr)
		upperExpr := strings.ToUpper(expr)

		// Check if this is a window function
		if strings.HasPrefix(upperExpr, "TUMBLE(") ||
			strings.HasPrefix(upperExpr, "HOP(") ||
			strings.HasPrefix(upperExpr, "SLIDING(") ||
			strings.HasPrefix(upperExpr, "SESSION(") {

			// Parse the window specification
			windowSpec, err := ParseTimeWindowSQL(expr)
			if err != nil {
				return nil, nil, err
			}

			// Remove this window function from group keys
			remainingKeys := make([]string, 0, len(groupByExprs)-1)
			remainingKeys = append(remainingKeys, groupByExprs[:i]...)
			remainingKeys = append(remainingKeys, groupByExprs[i+1:]...)

			return windowSpec, remainingKeys, nil
		}
	}

	// No time window found
	return nil, groupByExprs, nil
}
