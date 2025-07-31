/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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

package db

type DiscoveryType uint8

const (
	DiscoverFOFN DiscoveryType = iota
	DiscoverFOFNBase64
	DiscoverFOFNQuoted
	DiscoverFODN
	DiscoverFODNBase64
	DiscoverFODNQuoted
	DiscoverFile
	DiscoverDirectory
	DiscoverRemovedFile
	DiscoverRemovedDirectory
)

type Discover struct {
	Path string
	Type DiscoveryType
}

func (d *DB) AddSetDiscovery(set *Set, dr *Discover) error {
	return d.exec(createDiscover, set.id, dr.Path, dr.Type)
}

func (d *DBRO) GetSetDiscovery(set *Set) *IterErr[*Discover] {
	return iterRows(d, scanDiscover, getSetDiscovery, set.id)
}

func scanDiscover(scanner scanner) (*Discover, error) {
	d := new(Discover)

	if err := scanner.Scan(&d.Path, &d.Type); err != nil {
		return nil, err
	}

	return d, nil
}

func (d *DB) DeleteSetDiscovery(set *Set, path string) error {
	return d.exec(deleteDiscover, set.id, path)
}
