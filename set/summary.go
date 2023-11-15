/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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
	"fmt"
	"sort"
)

const bytesInTiB float64 = 1024 * 1024 * 1024 * 1024

// SizeAndNumber provides the total Size and Number of files in the set(s) this
// is a summary of, where "For" is the id of aggregation, eg it might be a set
// requester name if this was for a breakdown by requester.
//
// Files in the set(s) that began an upload (and we thus know their size) are
// part of these totals.
type SizeAndNumber struct {
	For    string // Aggregation id (set name or requester name or "total").
	Size   uint64 // Size in bytes.
	Number uint64 // Number of files.
}

// SizeTiB returns our Size in TiB.
func (s *SizeAndNumber) SizeTiB() float64 {
	return float64(s.Size) / bytesInTiB
}

// add increases our totals based on the info in the given set.
func (s *SizeAndNumber) add(set *Set) {
	s.Size += set.SizeFiles
	s.Number += set.NumFiles
}

// Usage is the type returned by UsageSummary() and contains various breakdowns
// of how much iRODS would be used up by the sets.
type Usage struct {
	// Total size and number of files across all given sets.
	Total *SizeAndNumber

	// ByRequester is a slice where each SizeAndNumber is the total size and
	// number of files for a particular requester amongst the given sets. The
	// slice is sorted from largest size to smallest.
	ByRequester []*SizeAndNumber

	// BySet is a slice where each SizeAndNumber is the total size and number of
	// files in each set. The "For" equals the set requester concatenated with
	// the set name. The slice is sorted from largest size to smallest.
	BySet []*SizeAndNumber

	// ByMonth is a slice where each SizeAndNumber is the total size and number
	// of files in sets that last completed in a certain calendar month. The
	// slice is sorted in date order. Incomplete sets are ignored.
	ByMonth []*SizeAndNumber
}

// UsageSummary returns a Usage that summaries potential iRODS usage of the
// given sets, broken down in various ways.
func UsageSummary(sets []*Set) *Usage {
	u := &Usage{
		Total: &SizeAndNumber{For: "Total"},
	}

	byRequester := make(map[string]*SizeAndNumber)
	bySet := make(map[string]*SizeAndNumber)
	byMonth := make(map[string]*SizeAndNumber)

	for _, set := range sets {
		u.Total.add(set)

		summariseByRequester(set, byRequester)
		summariseBySet(set, bySet)
		summariseByMonth(set, byMonth)
	}

	u.ByRequester = sortSANs(byRequester, sortBySize)
	u.BySet = sortSANs(bySet, sortBySize)
	u.ByMonth = sortSANs(byMonth, sortByFor)

	return u
}

func summariseByRequester(set *Set, byRequester map[string]*SizeAndNumber) {
	addToSANMap(set, set.Requester, byRequester)
}

func addToSANMap(set *Set, key string, sanMap map[string]*SizeAndNumber) {
	san, ok := sanMap[key]
	if !ok {
		san = &SizeAndNumber{For: key}
		sanMap[key] = san
	}

	san.add(set)
}

func summariseBySet(set *Set, bySet map[string]*SizeAndNumber) {
	addToSANMap(set, set.Requester+"."+set.Name, bySet)
}

func summariseByMonth(set *Set, byMonth map[string]*SizeAndNumber) {
	if set.LastCompleted.IsZero() {
		return
	}

	month := fmt.Sprintf("%d/%02d", set.LastCompleted.Year(), int(set.LastCompleted.Month()))

	addToSANMap(set, month, byMonth)
}

// sortSANs extracts the values from the given map and sorts them, largest
// first.
func sortSANs(sanMap map[string]*SizeAndNumber, s sorter) []*SizeAndNumber {
	sans := make([]*SizeAndNumber, 0, len(sanMap))

	for _, san := range sanMap {
		sans = append(sans, san)
	}

	sort.Slice(sans, func(i, j int) bool {
		return s(sans[i], sans[j])
	})

	return sans
}

type sorter func(sanI *SizeAndNumber, sanJ *SizeAndNumber) bool

func sortBySize(sanI *SizeAndNumber, sanJ *SizeAndNumber) bool {
	if sanI.Size == sanJ.Size {
		return sanI.Number > sanJ.Number
	}

	return sanI.Size > sanJ.Size
}

func sortByFor(sanI *SizeAndNumber, sanJ *SizeAndNumber) bool {
	return sanI.For <= sanJ.For
}
