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
	gas "github.com/wtsi-hgi/go-authserver"
)

const (
	testToken   = "TEST_TOKEN"
	testChannel = "#random"
	testMaxWait = 5 * time.Second
)

func TestRealSlack(t *testing.T) {
	token := os.Getenv("IBACKUP_SLACK_TOKEN")
	channel := os.Getenv("IBACKUP_SLACK_CHANNEL")

	if token == "" || channel == "" {
		t.Skip("IBACKUP_SLACK_TOKEN not set or IBACKUP_SLACK_CHANNEL not set")
	}

	logWriter := gas.NewStringLogger()

	Convey("You can send a message to real slack", t, func() {
		s := New(Config{Token: token, Channel: channel, ErrorLogger: logWriter})

		msg := "github.com/wtsi-hgi/ibackup slack package test"
		err := s.SendMessage(Info, msg)
		So(err, ShouldBeNil)

		<-time.After(1 * time.Second)

		So(logWriter.String(), ShouldBeBlank)
	})

	Convey("Bad token/channel results in error being logged", t, func() {
		config := Config{Token: "non", Channel: "sense", ErrorLogger: logWriter}
		s := New(config)

		msg := "github.com/wtsi-hgi/ibackup slack package error test"
		err := s.SendMessage(Info, msg)
		So(err, ShouldBeNil)

		<-time.After(1 * time.Second)

		So(logWriter.String(), ShouldEqual, "invalid_auth")

		Convey("And that works fine with no ErrorLogger", func() {
			config.ErrorLogger = nil
			s = New(config)

			err = s.SendMessage(Info, msg)
			So(err, ShouldBeNil)
			<-time.After(1 * time.Second)
		})
	})
}

func TestMockSlack(t *testing.T) {
	Convey("You can send a message to a mock slack server", t, func() {
		s, messageChan, dfunc := startMockSlackAndCreateSlack()
		testMessage := "test message"

		defer dfunc()

		err := s.SendMessage(Info, testMessage)
		So(err, ShouldBeNil)

		checkMessage(BoxPrefixInfo+testMessage, messageChan)
	})

	Convey("You can send different levels of message", t, func() {
		s, messageChan, dfunc := startMockSlackAndCreateSlack()
		testMessage := "test message"

		defer dfunc()

		err := s.SendMessage(Info, testMessage)
		So(err, ShouldBeNil)

		checkMessage(BoxPrefixInfo+testMessage, messageChan)

		err = s.SendMessage(Warn, testMessage)
		So(err, ShouldBeNil)

		checkMessage(BoxPrefixWarn+testMessage, messageChan)

		err = s.SendMessage(Error, testMessage)
		So(err, ShouldBeNil)

		checkMessage(BoxPrefixError+testMessage, messageChan)

		err = s.SendMessage(Success, testMessage)
		So(err, ShouldBeNil)

		checkMessage(BoxPrefixSuccess+testMessage, messageChan)
	})
}

func startMockSlackAndCreateSlack() (*Slack, chan *slackGo.MessageEvent, func()) {
	testServer := slacktest.NewTestServer()
	go testServer.Start()

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

	s := New(Config{Token: testToken, Channel: testChannel, URL: testServer.GetAPIURL()})

	return s, messageChan, testServer.Stop
}

func checkMessage(expectedMsg string, messageChan chan *slackGo.MessageEvent) {
	select {
	case m := <-messageChan:
		So(m.Channel, ShouldEqual, testChannel)
		So(m.Text, ShouldEqual, expectedMsg)

		break
	case <-time.After(testMaxWait):
		So(false, ShouldBeTrue, "did not get channel message in time")
	}
}
