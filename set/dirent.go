package set

import (
	"io/fs"

	"github.com/wtsi-ssg/wrstat/v6/walk"
)

type Dirent struct {
	Path  string
	Typ   fs.FileMode
	Inode uint64
}

func DirEntFromWalk(de *walk.Dirent) *Dirent {
	return &Dirent{
		Path:  string(de.Bytes()),
		Typ:   de.Type(),
		Inode: de.Inode,
	}
}
