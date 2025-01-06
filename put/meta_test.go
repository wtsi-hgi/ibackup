/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Author: Rosie Kern <rk18@sanger.ac.uk>
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
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMeta(t *testing.T) {
	Convey("Given meta strings", t, func() {
		validMetaStr1 := "ibackup:user:testKey1=testValue1"
		validMetaStr2 := "testKey2=testValue2"
		validMetaStr3 := "testKey3=testValue3;ibackup:user:testKey4=testValue4"
		validMetaStr4 := ""

		invalidMetaStr1 := "ibackup:testKey=testValue"
		invalidMetaStr2 := "testKey:testValue"

		Convey("You can get a Meta populated with the values", func() {
			meta, err := ParseMetaString(validMetaStr1)
			So(err, ShouldBeNil)
			So(meta.Metadata()["ibackup:user:testKey1"], ShouldEqual, "testValue1")

			meta, err = ParseMetaString(validMetaStr2)
			So(err, ShouldBeNil)
			So(meta.Metadata()["ibackup:user:testKey2"], ShouldEqual, "testValue2")

			meta, err = ParseMetaString(validMetaStr3)
			So(err, ShouldBeNil)
			So(meta.Metadata()["ibackup:user:testKey3"], ShouldEqual, "testValue3")
			So(meta.Metadata()["ibackup:user:testKey4"], ShouldEqual, "testValue4")

			meta, err = ParseMetaString(validMetaStr4)
			So(err, ShouldBeNil)
			So(meta, ShouldNotBeNil)
		})

		Convey("Invalid strings will be caught and an error returned", func() {
			_, err := ParseMetaString(invalidMetaStr1)
			So(err.Error(), ShouldContainSubstring, invalidMetaStr1)

			_, err = ParseMetaString(invalidMetaStr2)
			So(err.Error(), ShouldContainSubstring, invalidMetaStr2)
		})
	})

	Convey("Given a Meta", t, func() {
		meta := NewMeta()

		now := time.Now()

		Convey("And a reason, default review and removal dates are set", func() {
			defaultReview := now.AddDate(0, 6, 0).Format("2006-01-02")
			defaultRemoval := now.AddDate(1, 0, 0).Format("2006-01-02")

			err := createBackupMetadata(Backup, "", "", meta)
			So(err, ShouldBeNil)
			So(meta.Metadata()[MetaKeyReview], ShouldEqual, defaultReview)
			So(meta.Metadata()[MetaKeyRemoval], ShouldEqual, defaultRemoval)

			defaultReview = now.AddDate(1, 0, 0).Format("2006-01-02")
			defaultRemoval = now.AddDate(2, 0, 0).Format("2006-01-02")

			err = createBackupMetadata(Archive, "", "", meta)
			So(err, ShouldBeNil)
			So(meta.Metadata()[MetaKeyReview], ShouldEqual, defaultReview)
			So(meta.Metadata()[MetaKeyRemoval], ShouldEqual, defaultRemoval)

			defaultReview = now.AddDate(0, 2, 0).Format("2006-01-02")
			defaultRemoval = now.AddDate(0, 3, 0).Format("2006-01-02")

			err = createBackupMetadata(Quarantine, "", "", meta)
			So(err, ShouldBeNil)
			So(meta.Metadata()[MetaKeyReview], ShouldEqual, defaultReview)
			So(meta.Metadata()[MetaKeyRemoval], ShouldEqual, defaultRemoval)
		})

		Convey("And a review/removal date, invalid dates return an error", func() {
			err := createBackupMetadata(Backup, "2y", "3y", meta)
			So(err, ShouldBeNil)

			err = createBackupMetadata(Backup, "2y", "1y", meta)
			So(err, ShouldEqual, ErrInvalidReviewRemoveDate)

			err = createBackupMetadata(Backup, "2y", "", meta)
			So(err, ShouldEqual, ErrInvalidReviewRemoveDate)

			err = createBackupMetadata(Backup, "", "1m", meta)
			So(err, ShouldEqual, ErrInvalidReviewRemoveDate)

			err = createBackupMetadata(Backup, "1m", "1m", meta)
			So(err, ShouldEqual, ErrInvalidReviewRemoveDate)

			err = createBackupMetadata(Backup, "2", "1", meta)
			So(err, ShouldEqual, ErrInvalidDurationFormat)
		})
	})

	Convey("Given a valid meta string, reason, review date and remove date", t, func() {
		metaString := "ibackup:user:testKey=testValue"
		reason := Backup
		review := "1y"
		remove := "2y"

		Convey("You can create a Meta containing all the provided metadata", func() {
			meta, err := HandleMeta(metaString, reason, review, remove)
			So(err, ShouldBeNil)

			So(meta.Metadata(), ShouldResemble, map[string]string{
				"ibackup:user:testKey": "testValue",
				"ibackup:reason":       "backup",
				"ibackup:review":       time.Now().AddDate(1, 0, 0).Format("2006-01-02"),
				"ibackup:removal":      time.Now().AddDate(2, 0, 0).Format("2006-01-02"),
			})

			Convey("Which you can append metadata to", func() {
				meta.appendMeta("ibackup:user:testKey2", "testValue2")
				So(meta.Metadata(), ShouldResemble, map[string]string{
					"ibackup:user:testKey":  "testValue",
					"ibackup:reason":        "backup",
					"ibackup:review":        time.Now().AddDate(1, 0, 0).Format("2006-01-02"),
					"ibackup:removal":       time.Now().AddDate(2, 0, 0).Format("2006-01-02"),
					"ibackup:user:testKey2": "testValue2",
				})
			})
		})
	})
}
