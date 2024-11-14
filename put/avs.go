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

// Len returns the number of AV elements in the AVs.
func (a *AVs) Len() int {
	return len(a.avs)
}

// Resembles checks if the current AVs instance resembles another AVs instance.
// Two AVs resemble each other if they have the same attribute-value pairs regardless of order.
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

// AttrResembles checks if the values of a specific attribute in the current AVs
// instance resemble those in another AVs instance. It returns true if both
// instances contain identical values for the attribute.
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

// Get retrieves all values associated with a specified attribute in the AVs instance.
func (a *AVs) Get(attribute string) []string {
	var values []string

	for _, av := range a.avs {
		if av.Attr == attribute {
			values = append(values, av.Val)
		}
	}

	return values
}

// GetSingle retrieves a single value associated with a specified attribute in the AVs instance.
// It returns an empty string if there are no or several values for that attribute.
func (a *AVs) GetSingle(attribute string) string {
	values := a.Get(attribute)
	if len(values) != 1 {
		return ""
	}

	return values[0]
}

// Add appends a new attribute-value pair to the AVs instance.
// Won't do anything if the same pair already exists.
func (a *AVs) Add(attribute, value string) {
	av := AV{Attr: attribute, Val: value}
	if !a.Contains(av) {
		a.avs = append(a.avs, av)
	}
}

// Set sets the value for a specified attribute in the AVs instance.
// It removes any existing entries with that attribute first.
func (a *AVs) Set(attribute, value string) {
	a.Remove(attribute)
	a.Add(attribute, value)
}

// Remove deletes all occurrences of a specified attribute from the AVs instance.
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

// Clone returns a deep copy of the current AVs instance.
func (a *AVs) Clone() *AVs {
	clonedAVs := make([]AV, len(a.avs))

	copy(clonedAVs, a.avs)

	return NewAVs(clonedAVs...)
}

// Contains check that the given AV is present in AVs.
func (a *AVs) Contains(av AV) bool {
	found := false

	for avA := range a.Iter() {
		if avA == av {
			found = true

			break
		}
	}

	return found
}

// Iter returns a channel that allows iteration over each AV in the AVs instance.
// The channel is closed automatically when all elements have been sent.
func (a *AVs) Iter() <-chan AV {
	ch := make(chan AV)
	go func() {
		defer close(ch)

		for _, av := range a.avs {
			ch <- av
		}
	}()

	return ch
}

// DetermineMetadataToRemoveAndAdd finds the metadata that should be added to or removed from a to make it match b.
func DetermineMetadataToRemoveAndAdd(a, b *AVs) (*AVs, *AVs) {
	metaToAdd := NewAVs()
	metaToRemove := NewAVs()

	var found bool

	for avB := range b.Iter() {
		found = a.Contains(avB)

		if !found {
			metaToAdd.Add(avB.Attr, avB.Val)
		}
	}

	for avA := range a.Iter() {
		found = b.Contains(avA)

		if !found {
			metaToRemove.Add(avA.Attr, avA.Val)
		}
	}

	return metaToAdd, metaToRemove
}
