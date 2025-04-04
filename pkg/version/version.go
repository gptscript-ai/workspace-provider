package version

import (
	"fmt"
	"runtime/debug"
)

var (
	Tag         = "v0.0.0-dev"
	ProgramName = "workspace-provider"
)

func Get() Version {
	v := Version{
		Tag: Tag,
	}
	v.Commit, v.Dirty = GitCommit()
	return v
}

type Version struct {
	Tag    string `json:"tag,omitempty"`
	Commit string `json:"commit,omitempty"`
	Dirty  bool   `json:"dirty,omitempty"`
}

func (v Version) String() string {
	if len(v.Commit) < 12 {
		return v.Tag
	} else if v.Dirty {
		return fmt.Sprintf("%s-%s-dirty", v.Tag, v.Commit[:8])
	}

	return fmt.Sprintf("%s+%s", v.Tag, v.Commit[:8])
}

func GitCommit() (commit string, dirty bool) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "", false
	}
	for _, setting := range bi.Settings {
		switch setting.Key {
		case "vcs.modified":
			dirty = setting.Value == "true"
		case "vcs.revision":
			commit = setting.Value
		}
	}

	return
}
