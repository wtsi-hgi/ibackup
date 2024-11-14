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

import (
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAVs(t *testing.T) {
	Convey("Given a slice of AV structs", t, func() {
		rawAVs := []AV{
			{"key1", "val1"},
			{"key2", "val2"},
			{"key1", "val3"},
			{"key3", "val4"},
		}

		Convey("You can make an AVs struct", func() {
			avs := NewAVs(rawAVs...)

			Convey("and get its length", func() {
				So(avs.Len(), ShouldEqual, 4)
			})

			Convey("and get single attribute value", func() {
				So(avs.GetSingle("key2"), ShouldEqual, "val2")
				So(avs.GetSingle("key1"), ShouldEqual, "")
				So(avs.GetSingle("key3"), ShouldEqual, "val4")
			})

			Convey("and get all values for given attributes", func() {
				So(avs.Get("key2"), ShouldResemble, []string{"val2"})
				So(avs.Get("key1"), ShouldResemble, []string{"val1", "val3"})
			})

			Convey("and add a new AV", func() {
				avs.Add("key4", "val5")
				So(avs.Len(), ShouldEqual, 5)
				So(avs.GetSingle("key4"), ShouldEqual, "val5")

				avs.Add("key4", "val5")
				So(avs.Len(), ShouldEqual, 5)
			})

			Convey("and remove an AV", func() {
				avs.Remove("key3")
				So(avs.Len(), ShouldEqual, 3)
				So(avs.GetSingle("key3"), ShouldBeBlank)

				avs.Remove("key1")
				So(avs.Len(), ShouldEqual, 1)
				So(avs.Get("key1"), ShouldBeNil)
			})

			Convey("and set values", func() {
				avs.Set("key1", "val5")
				So(avs.Len(), ShouldEqual, 3)
				So(avs.Get("key1"), ShouldResemble, []string{"val5"})
			})

			Convey("and check if it resembles another AVs", func() {
				rawAVs2 := make([]AV, len(rawAVs))
				copy(rawAVs2, rawAVs)
				avs2 := NewAVs(rawAVs2...)
				So(avs.Resembles(avs2), ShouldBeTrue)

				avs2.Remove("key2")
				So(avs.Resembles(avs2), ShouldBeFalse)

				avs2.Set("key2", "val2")
				So(avs.Resembles(avs2), ShouldBeTrue)

				avs2.Remove("key1")
				So(avs.Resembles(avs2), ShouldBeFalse)

				avs2.Add("key1", "val1")
				So(avs.Resembles(avs2), ShouldBeFalse)

				avs2.Add("key1", "val2")
				So(avs.Resembles(avs2), ShouldBeFalse)

				Convey("and check if its Attr resembles another one", func() {
					So(avs.AttrResembles("key2", avs2), ShouldBeTrue)
					So(avs.AttrResembles("key1", avs2), ShouldBeFalse)
				})

				Convey("and compare with another instance", func() {
					avs2.Remove("key1")
					avs2.Set("key4", "val1")

					add, remove := DetermineMetadataToRemoveAndAdd(avs, avs2)

					So(add.Len(), ShouldEqual, 1)
					So(remove.Len(), ShouldEqual, 2)
				})
			})

			Convey("and clone it", func() {
				avs2 := avs.Clone()
				So(avs2.Resembles(avs), ShouldBeTrue)

				avs2.Remove("key2")
				So(avs2.Resembles(avs), ShouldBeFalse)

				So(fmt.Sprintf("%p", avs.avs), ShouldNotEqual, fmt.Sprintf("%p", avs2.avs))
			})

			Convey("and iterate over it", func() {
				for av := range avs.Iter() {
					So(av.Attr, ShouldContainSubstring, "key")
					So(av.Val, ShouldContainSubstring, "val")
				}
			})

			Convey("and check the presence of AV", func() {
				So(avs.Contains(AV{"key1", "val1"}), ShouldBeTrue)
				So(avs.Contains(AV{"key1", "val2"}), ShouldBeFalse)
				So(avs.Contains(AV{"key4", "val1"}), ShouldBeFalse)
			})
		})
	})

	Convey("You can make an AVs with no elements", t, func() {
		avs := NewAVs()
		So(len(avs.avs), ShouldEqual, 0)
		So(avs.Len(), ShouldEqual, 0)
		avs.Add("key1", "val1")
		So(avs.Len(), ShouldEqual, 1)
	})
}
