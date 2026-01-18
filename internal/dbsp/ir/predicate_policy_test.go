package ir

import "testing"

func TestBuildPredicateFunc_TypeAndNullPolicy(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		tup  map[string]any
		want bool
	}{
		{
			name: "numeric-string-gte-true",
			sql:  "v >= 10",
			tup:  map[string]any{"v": "10"},
			want: true,
		},
		{
			name: "numeric-string-gte-false",
			sql:  "v >= 10",
			tup:  map[string]any{"v": "9"},
			want: false,
		},
		{
			name: "float-gte-true",
			sql:  "v >= 10",
			tup:  map[string]any{"v": 10.0},
			want: true,
		},
		{
			name: "null-comparison-false",
			sql:  "v >= 10",
			tup:  map[string]any{"v": nil},
			want: false,
		},
		{
			name: "missing-comparison-false",
			sql:  "v >= 10",
			tup:  map[string]any{},
			want: false,
		},
		{
			name: "numeric-eq-true-string",
			sql:  "v = 10",
			tup:  map[string]any{"v": "10"},
			want: true,
		},
		{
			name: "numeric-eq-true-float",
			sql:  "v = 10",
			tup:  map[string]any{"v": 10.0},
			want: true,
		},
		{
			name: "numeric-eq-false-nonnumeric",
			sql:  "v = 10",
			tup:  map[string]any{"v": "ten"},
			want: false,
		},
		{
			name: "string-eq-true",
			sql:  "k = 'A'",
			tup:  map[string]any{"k": "A"},
			want: true,
		},
		{
			name: "string-eq-false",
			sql:  "k = 'A'",
			tup:  map[string]any{"k": "B"},
			want: false,
		},
		{
			name: "is-null-missing-true",
			sql:  "x IS NULL",
			tup:  map[string]any{},
			want: true,
		},
		{
			name: "is-null-nil-true",
			sql:  "x IS NULL",
			tup:  map[string]any{"x": nil},
			want: true,
		},
		{
			name: "is-null-non-nil-false",
			sql:  "x IS NULL",
			tup:  map[string]any{"x": ""},
			want: false,
		},
		{
			name: "is-not-null-missing-false",
			sql:  "x IS NOT NULL",
			tup:  map[string]any{},
			want: false,
		},
		{
			name: "is-not-null-nil-false",
			sql:  "x IS NOT NULL",
			tup:  map[string]any{"x": nil},
			want: false,
		},
		{
			name: "is-not-null-empty-string-true",
			sql:  "x IS NOT NULL",
			tup:  map[string]any{"x": ""},
			want: true,
		},
		{
			name: "paren-and-or-precedence",
			sql:  "(v >= 10 AND k = 'A') OR k = 'B'",
			tup:  map[string]any{"v": "9", "k": "B"},
			want: true,
		},
		{
			name: "paren-and-or-precedence-false",
			sql:  "(v >= 10 AND k = 'A') OR k = 'B'",
			tup:  map[string]any{"v": "9", "k": "C"},
			want: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			pred := BuildPredicateFunc(tc.sql)
			got := pred(tc.tup)
			if got != tc.want {
				t.Fatalf("predicate %q on tuple %v: got=%v want=%v", tc.sql, tc.tup, got, tc.want)
			}
		})
	}
}
