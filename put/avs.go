/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Author: Iaroslav Popov <ip13@sanger.ac.uk>
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

package put

type AV struct {
	Attr string
	Val  string
}

type AVs struct {
	avs []AV
}

func NewAVs(avs ...AV) *AVs {
	return &AVs{avs: avs}
}

func (a *AVs) Len() int {
	return len(a.avs)
}

func (a *AVs) Resembles(cmp *AVs) bool { //nolint:gocyclo
	if len(a.avs) != len(cmp.avs) {
		return false
	}

	countsA := make(map[AV]int)
	countsB := make(map[AV]int)

	for _, av := range a.avs {
		countsA[av]++
	}

	for _, av := range cmp.avs {
		countsB[av]++
	}

	if len(countsA) != len(countsB) {
		return false
	}

	for key, countA := range countsA {
		if countB, found := countsB[key]; !found || countA != countB {
			return false
		}
	}

	return true
}

func (a *AVs) AttrResembles(attribute string, cmp *AVs) bool {
	valuesA := a.Get(attribute)
	valuesB := cmp.Get(attribute)

	countMap := make(map[string]int)

	for _, v := range valuesA {
		countMap[v]++
	}

	for _, v := range valuesB {
		countMap[v]--
		if countMap[v] == 0 {
			delete(countMap, v)
		}
	}

	return len(countMap) == 0
}

func (a *AVs) Get(attribute string) []string {
	var values []string

	for _, av := range a.avs {
		if av.Attr == attribute {
			values = append(values, av.Val)
		}
	}

	return values
}

func (a *AVs) GetSingle(attribute string) string {
	values := a.Get(attribute)
	if len(values) != 1 {
		return ""
	}

	return values[0]
}

func (a *AVs) Add(attribute, value string) {
	av := AV{Attr: attribute, Val: value}
	a.avs = append(a.avs, av)
}

func (a *AVs) Set(attribute, value string) {
	a.Remove(attribute)
	a.Add(attribute, value)
}

func (a *AVs) Remove(attribute string) {
	writeIdx := 0

	for _, av := range a.avs {
		if av.Attr != attribute {
			a.avs[writeIdx] = av
			writeIdx++
		}
	}

	a.avs = a.avs[:writeIdx]
}

func (a *AVs) Clone() *AVs {
	clonedAVs := make([]AV, len(a.avs))

	copy(clonedAVs, a.avs)

	return NewAVs(clonedAVs...)
}

func DetermineMetadataToRemoveAndAdd(a, b *AVs) (*AVs, *AVs) {
	return &AVs{}, &AVs{}
}
