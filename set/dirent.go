/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
package set

import (
	"io/fs"

	"github.com/wtsi-ssg/wrstat/v6/walk"
)

// Dirent represents and entry produced from a walk of a specified directory.
type Dirent struct {
	Path  string
	Mode  fs.FileMode
	Inode uint64
}

// DirEntFromWalk converts a walk.Dirent to an easier to use local Dirent type.
func DirEntFromWalk(de *walk.Dirent) *Dirent {
	return &Dirent{
		Path:  string(de.Bytes()),
		Mode:  de.Type(),
		Inode: de.Inode,
	}
}

// IsRegular returns true if the entry refers to a regular file.
func (d *Dirent) IsRegular() bool {
	return d.Mode.IsRegular()
}

// IsDir returns true if the entry refers to a directory.
func (d *Dirent) IsDir() bool {
	return d.Mode.IsDir()
}

// IsIrregular returns true if the entry refers to an irregular file.
func (d *Dirent) IsIrregular() bool {
	return d.Mode == fs.ModeIrregular
}

// IsSymlink returns true if the entry refers to a symlink.
func (d *Dirent) IsSymlink() bool {
	return d.Mode == fs.ModeSymlink
}

// IsSocket returns true if the entry refers to a socket.
func (d *Dirent) IsSocket() bool {
	return d.Mode == fs.ModeSocket
}

// IsDevice returns true if the entry refers to a device node.
func (d *Dirent) IsDevice() bool {
	return d.Mode == fs.ModeDevice
}

// IsCharDevice returns true if the entry refers to a character device node.
func (d *Dirent) IsCharDevice() bool {
	return d.Mode == fs.ModeCharDevice
}

// IsPipe returns true if the entry refers to a named (FIFO) pipe.
func (d *Dirent) IsPipe() bool {
	return d.Mode == fs.ModeNamedPipe
}
