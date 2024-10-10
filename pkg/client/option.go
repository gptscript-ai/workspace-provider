package client

// LsOptions is the options for the ls command.
// Note: Ls is, by default, recursive.
type LsOptions struct {
	// List files starting in the subdirectory.
	SubDir string `usage:"The subdirectory to list" env:"LS_SUB_DIR"`
	// Don't recurse into subdirectories.
	NonRecursive bool `usage:"Don't recurse into subdirectories" env:"LS_NON_RECURSIVE"`
	// Exclude hidden files (i.e. files that start with a dot).
	ExcludeHidden bool `usage:"Exclude hidden files" env:"LS_EXCLUDE_HIDDEN"`
}

type WriteOptions struct {
	// Don't create the file if it doesn't exist.
	WithoutCreate bool `usage:"Don't create the file if it doesn't exist" env:"WRITE_FILE_WITHOUT_CREATE"`
	// Requires that the file doesn't exist before writing.
	// If MustNotExist is set, then WithoutCreate is ignored.
	MustNotExist bool `usage:"Requires that the file doesn't exist before writing" env:"WRITE_FILE_MUST_NOT_EXIST"`
	// Create the parent directories if they don't exist.
	CreateDirs bool `usage:"Create the parent directories if they don't exist" env:"WRITE_FILE_CREATE_DIRS"`
}

type MkDirOptions struct {
	// Requires that the directory doesn't exist before creating.
	MustNotExist bool `usage:"Requires that the directory doesn't exist before creating" env:"MK_DIR_MUST_NOT_EXIST"`
	// Recursively create the parent directories if they don't exist.
	CreateDirs bool `usage:"Recursively create the parent directories if they don't exist" env:"MK_DIR_CREATE_DIRS"`
}

type RmDirOptions struct {
	// Only delete the directory it if is non-empty.
	NonEmpty bool `usage:"Only delete the directory it if it is non-empty" env:"RM_DIR_NON_EMPTY"`
}
