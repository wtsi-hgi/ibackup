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

package transfer

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMeta(t *testing.T) {
	now := time.Now()

	Convey("Given meta strings", t, func() {
		validMetaStr1 := "ibackup:user:testKey1=testValue1"
		validMetaStr2 := "testKey2=testValue2"
		validMetaStr3 := "testKey3=testValue3;ibackup:user:testKey4=testValue4"
		validMetaStr4 := ""
		validPrevMetaMap := map[string]string{
			"ibackup:user:testKeyPrev1": "testValuePrev1",
			"ibackup:user:testKeyPrev2": "testValuePrev2",
		}

		invalidMetaStr1 := "ibackup:testKey=testValue"
		invalidMetaStr2 := "testKey:testValue"

		Convey("You can get a Meta populated with the values", func() {
			meta, err := ParseMetaString(validMetaStr1, nil)
			So(err, ShouldBeNil)
			So(meta.Metadata()["ibackup:user:testKey1"], ShouldEqual, "testValue1")

			meta, err = ParseMetaString(validMetaStr2, nil)
			So(err, ShouldBeNil)
			So(meta.Metadata()["ibackup:user:testKey2"], ShouldEqual, "testValue2")

			meta, err = ParseMetaString(validMetaStr3, validPrevMetaMap)
			So(err, ShouldBeNil)
			So(meta.Metadata()["ibackup:user:testKey3"], ShouldEqual, "testValue3")
			So(meta.Metadata()["ibackup:user:testKey4"], ShouldEqual, "testValue4")

			meta, err = ParseMetaString(validMetaStr4, nil)
			So(err, ShouldBeNil)
			So(meta, ShouldNotBeNil)

			meta, err = ParseMetaString(validMetaStr4, validPrevMetaMap)
			So(err, ShouldBeNil)
			So(meta.Metadata()["ibackup:user:testKeyPrev1"], ShouldEqual, "testValuePrev1")
			So(meta.Metadata()["ibackup:user:testKeyPrev2"], ShouldEqual, "testValuePrev2")
		})

		Convey("Invalid strings will be caught and an error returned", func() {
			_, err := ParseMetaString(invalidMetaStr1, nil)
			So(err.Error(), ShouldContainSubstring, invalidMetaStr1)

			_, err = ParseMetaString(invalidMetaStr2, validPrevMetaMap)
			So(err.Error(), ShouldContainSubstring, invalidMetaStr2)
		})
	})

	Convey("Given a Meta", t, func() {
		meta := NewMeta()

		Convey("And a reason, default review and removal dates are set", func() {
			defaultReview, defaultRemoval := testTimesToMeta(t, now.AddDate(0, 6, 0), now.AddDate(1, 0, 0))

			err := createBackupMetadata(Backup, "", "", meta)
			So(err, ShouldBeNil)
			So(meta.Metadata()[MetaKeyReview], ShouldEqual, defaultReview)
			So(meta.Metadata()[MetaKeyRemoval], ShouldEqual, defaultRemoval)

			defaultReview, defaultRemoval = testTimesToMeta(t, now.AddDate(1, 0, 0), now.AddDate(2, 0, 0))

			err = createBackupMetadata(Archive, "", "", meta)
			So(err, ShouldBeNil)
			So(meta.Metadata()[MetaKeyReview], ShouldEqual, defaultReview)
			So(meta.Metadata()[MetaKeyRemoval], ShouldEqual, defaultRemoval)

			defaultReview, defaultRemoval = testTimesToMeta(t, now.AddDate(0, 2, 0), now.AddDate(0, 3, 0))

			err = createBackupMetadata(Quarantine, "", "", meta)
			So(err, ShouldBeNil)
			So(meta.Metadata()[MetaKeyReview], ShouldEqual, defaultReview)
			So(meta.Metadata()[MetaKeyRemoval], ShouldEqual, defaultRemoval)
		})

		Convey("Valid review/removal inputs return the correct date", func() {
			defaultReview, defaultRemoval := testTimesToMeta(t, now.AddDate(2, 0, 0), now.AddDate(3, 0, 0))

			err := createBackupMetadata(Backup, "2y", "3y", meta)
			So(err, ShouldBeNil)
			So(meta.Metadata()[MetaKeyReview], ShouldEqual, defaultReview)
			So(meta.Metadata()[MetaKeyRemoval], ShouldEqual, defaultRemoval)

			customReview := "2050-01-01"
			customRemoval := "2055-01-01"

			err = createBackupMetadata(Backup, customReview, customRemoval, meta)
			So(err, ShouldBeNil)
			So(meta.Metadata()[MetaKeyReview], ShouldContainSubstring, customReview)
			So(meta.Metadata()[MetaKeyRemoval], ShouldContainSubstring, customRemoval)
		})

		Convey("Invalid review/removal inputs return an error", func() {
			err := createBackupMetadata(Backup, "2y", "1y", meta)
			So(err.Error(), ShouldContainSubstring, ErrInvalidReviewRemoveDate)

			err = createBackupMetadata(Backup, "2y", "", meta)
			So(err.Error(), ShouldContainSubstring, ErrInvalidReviewRemoveDate)

			err = createBackupMetadata(Backup, "", "1m", meta)
			So(err.Error(), ShouldContainSubstring, ErrInvalidReviewRemoveDate)

			err = createBackupMetadata(Backup, "1m", "1m", meta)
			So(err.Error(), ShouldContainSubstring, ErrInvalidReviewRemoveDate)

			err = createBackupMetadata(Backup, "2", "1", meta)
			So(err.Error(), ShouldContainSubstring, ErrInvalidDurationFormat)
		})
	})

	Convey("Given a valid meta string, reason, review date and remove date", t, func() {
		metaString := "ibackup:user:testKey=testValue"
		reason := Backup
		review := "1y"
		remove := "2y"

		Convey("You can create a Meta containing all the provided metadata", func() {
			meta, err := HandleMeta(metaString, reason, review, remove, nil)
			So(err, ShouldBeNil)

			reviewDate, removalDate := testTimesToMeta(t, now.AddDate(1, 0, 0), now.AddDate(2, 0, 0))

			So(meta.Metadata(), ShouldResemble, map[string]string{
				"ibackup:user:testKey": "testValue",
				"ibackup:reason":       "backup",
				"ibackup:review":       reviewDate,
				"ibackup:removal":      removalDate,
			})

			Convey("Which you can append metadata to", func() {
				meta.appendMeta("ibackup:user:testKey2", "testValue2")
				So(meta.Metadata(), ShouldResemble, map[string]string{
					"ibackup:user:testKey":  "testValue",
					"ibackup:reason":        "backup",
					"ibackup:review":        reviewDate,
					"ibackup:removal":       removalDate,
					"ibackup:user:testKey2": "testValue2",
				})
			})
		})
	})
}

func testTimesToMeta(t *testing.T, reviewDate, removalDate time.Time) (string, string) {
	t.Helper()

	reviewStr, err := TimeToMeta(reviewDate)
	So(err, ShouldBeNil)

	removalStr, err := TimeToMeta(removalDate)
	So(err, ShouldBeNil)

	return reviewStr, removalStr
}
