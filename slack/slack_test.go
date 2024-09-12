/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
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

package slack

import (
	"os"
	"testing"
	"time"

	slackGo "github.com/slack-go/slack"
	"github.com/slack-go/slack/slacktest"
	. "github.com/smartystreets/goconvey/convey"
)

func TestRealSlack(t *testing.T) {
	token := os.Getenv("IBACKUP_SLACK_TOKEN")
	channel := os.Getenv("IBACKUP_SLACK_CHANNEL")

	if token == "" || channel == "" {
		t.Skip("IBACKUP_SLACK_TOKEN not set or IBACKUP_SLACK_CHANNEL not set")
	}

	Convey("You can send a message to real slack", t, func() {
		s := New(Config{Token: token, Channel: channel})

		msg := "test"
		err := s.SendMessage(msg)
		So(err, ShouldBeNil)
	})
}

func TestMockSlack(t *testing.T) {
	testToken := "TEST_TOKEN"

	Convey("You can send a message to a mock slack server", t, func() {
		testServer := slacktest.NewTestServer()
		go testServer.Start()
		defer testServer.Stop()

		maxWait := 5 * time.Second

		api := slackGo.New(testToken, slackGo.OptionAPIURL(testServer.GetAPIURL()))
		rtm := api.NewRTM()

		go rtm.ManageConnection()

		messageChan := make(chan (*slackGo.MessageEvent), 1)

		go func() {
			for msg := range rtm.IncomingEvents {
				if ev, ok := msg.Data.(*slackGo.MessageEvent); ok {
					messageChan <- ev
				}
			}
		}()

		testChannel := "#random"
		testMessage := "test message"

		s := New(Config{Token: testToken, Channel: testChannel, URL: testServer.GetAPIURL()})

		err := s.SendMessage(testMessage)
		So(err, ShouldBeNil)

		select {
		case m := <-messageChan:
			So(m.Channel, ShouldEqual, testChannel)
			So(m.Text, ShouldEqual, testMessage)

			break
		case <-time.After(maxWait):
			So(false, ShouldBeTrue, "did not get channel message in time")
		}
	})
}
