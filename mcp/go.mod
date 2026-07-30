module github.com/aleks-drozy/jobq/mcp

go 1.26.5

require (
	github.com/aleks-drozy/jobq v0.0.0-00010101000000-000000000000
	github.com/modelcontextprotocol/go-sdk v1.7.0
)

require (
	github.com/google/jsonschema-go v0.4.3 // indirect
	github.com/segmentio/asm v1.1.3 // indirect
	github.com/segmentio/encoding v0.5.4 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	golang.org/x/oauth2 v0.35.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/time v0.15.0 // indirect
)

// This module lives in the same repo as its parent and always builds
// against the current checkout, not a tagged release — a mono-repo nested
// module tracking its own parent by version would need a tag bump on every
// root-module change just to pick it up.
replace github.com/aleks-drozy/jobq => ../
