package client

// LsOptions is the options for the ls command.
// Note: Ls is, by default, recursive.
type LsOptions struct {
	// List files starting in the subdirectory.
	SubDir string
	// Don't recurse into subdirectories.
	NonRecursive bool
	// Exclude hidden files (i.e. files that start with a dot).
	ExcludeHidden bool
}

type WriteOptions struct {
	// Don't create the file if it doesn't exist.
	WithoutCreate bool
	// Requires that the file doesn't exist before writing.
	// If MustNotExist is set, then WithoutCreate is ignored.
	MustNotExist bool
	// Create the parent directories if they don't exist.
	CreateDirs bool
}
