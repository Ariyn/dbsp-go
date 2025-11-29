module github.com/ariyn/dbsp

go 1.25

require github.com/Ariyn/tree-sitter-duckdb/bindings/go v0.0.0-20251129114914-9fc775caffba

require github.com/smacker/go-tree-sitter v0.0.0-20240827094217-dd81d9e9be82 // indirect

replace github.com/Ariyn/tree-sitter-duckdb/bindings/go => ./reference/tree-sitter-duckdb/bindings/go
