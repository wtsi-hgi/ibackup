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
	"fmt"
	"os"
	"testing"
	"time"

	slackGo "github.com/slack-go/slack"
	"github.com/slack-go/slack/slacktest"
	. "github.com/smartystreets/goconvey/convey"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/internal/testutil"
)

const (
	testToken          = "TEST_TOKEN"
	testChannel        = "#random"
	testMaxWait        = 5 * time.Second
	testSmallerMaxWait = 300 * time.Millisecond
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
		s.SendMessage(Info, msg)

		testutil.Eventually(t, 2*time.Second, 25*time.Millisecond, func() bool {
			return logWriter.String() == ""
		}, "slack log to remain blank")

		So(logWriter.String(), ShouldBeBlank)
	})

	Convey("Bad token/channel results in error being logged", t, func() {
		config := Config{Token: "non", Channel: "sense", ErrorLogger: logWriter}
		s := New(config)

		msg := "github.com/wtsi-hgi/ibackup slack package error test"
		s.SendMessage(Info, msg)

		testutil.Eventually(t, 2*time.Second, 25*time.Millisecond, func() bool {
			return logWriter.String() == "invalid_auth"
		}, "slack invalid_auth log")

		So(logWriter.String(), ShouldEqual, "invalid_auth")

		Convey("And that works fine with no ErrorLogger", func() {
			config.ErrorLogger = nil
			s = New(config)

			s.SendMessage(Info, msg)
			testutil.Eventually(t, 2*time.Second, 25*time.Millisecond, func() bool {
				return true
			}, "slack message sent with no logger")
		})
	})
}

func TestDebounce(t *testing.T) {
	testMessageSuffix := "test messages"

	debounce := 500 * time.Millisecond

	Convey("Without a slacker you can still create a max value debouncer", t, func() {
		hnd := NewHighestNumDebouncer(nil, debounce, testMessageSuffix)

		Convey("Which doesn't panic when SendDebounceMsg is called", func() {
			hnd.SendDebounceMsg(1)
		})
	})

	Convey("Given a slacker", t, func() {
		s, messageChan, dfunc := startMockSlackAndCreateSlack()
		defer dfunc()

		Convey("You can create a highest number debouncer", func() {
			hnd := NewHighestNumDebouncer(s, debounce, testMessageSuffix)

			Convey("Which only sends messages after the debounce timeout", func() {
				hnd.SendDebounceMsg(1)

				expectedOutput := fmt.Sprintf("%s1 %s", BoxPrefixInfo, testMessageSuffix)
				checkMessage(t, expectedOutput, messageChan)

				hnd.SendDebounceMsg(2)
				So(checkNoMessage(t, messageChan), ShouldBeTrue)

				expectedOutput = fmt.Sprintf("%s2 %s", BoxPrefixInfo, testMessageSuffix)
				checkMessage(t, expectedOutput, messageChan)
			})

			Convey("Which only sends unique messages", func() {
				hnd.SendDebounceMsg(1)

				expectedOutput := fmt.Sprintf("%s1 %s", BoxPrefixInfo, testMessageSuffix)
				checkMessage(t, expectedOutput, messageChan)

				hnd.SendDebounceMsg(1)
				So(checkNoMessage(t, messageChan), ShouldBeTrue)

				testutil.RequireStable(t, debounce, 10*time.Millisecond, func() bool {
					select {
					case <-messageChan:
						return false
					default:
						return true
					}
				}, "no debounce message for duplicate")
			})

			Convey("Which sends the highest number seen in the debounce period", func() {
				hnd.SendDebounceMsg(4)

				expectedOutput := fmt.Sprintf("%s4 %s", BoxPrefixInfo, testMessageSuffix)
				checkMessage(t, expectedOutput, messageChan)

				hnd.SendDebounceMsg(3)
				hnd.SendDebounceMsg(0)
				hnd.SendDebounceMsg(6)
				hnd.SendDebounceMsg(5)

				expectedOutput = fmt.Sprintf("%s6 %s", BoxPrefixInfo, testMessageSuffix)
				checkMessage(t, expectedOutput, messageChan)
			})

			Convey("Which always sends the final 0 message", func() {
				hnd.SendDebounceMsg(1)

				expectedOutput := fmt.Sprintf("%s1 %s", BoxPrefixInfo, testMessageSuffix)
				checkMessage(t, expectedOutput, messageChan)

				hnd.SendDebounceMsg(3)

				expectedOutput = fmt.Sprintf("%s3 %s", BoxPrefixInfo, testMessageSuffix)
				checkMessage(t, expectedOutput, messageChan)

				hnd.SendDebounceMsg(2)
				hnd.SendDebounceMsg(0)

				expectedOutput = fmt.Sprintf("%s2 %s", BoxPrefixInfo, testMessageSuffix)
				checkMessage(t, expectedOutput, messageChan)

				expectedOutput = fmt.Sprintf("%s0 %s", BoxPrefixInfo, testMessageSuffix)
				checkMessage(t, expectedOutput, messageChan)
			})
		})
	})
}

func TestMockSlack(t *testing.T) {
	Convey("You can send a message to a mock slack server", t, func() {
		s, messageChan, dfunc := startMockSlackAndCreateSlack()
		testMessage := "test message"

		defer dfunc()

		s.SendMessage(Info, testMessage)
		checkMessage(t, BoxPrefixInfo+testMessage, messageChan)
	})

	Convey("You can send different levels of message", t, func() {
		s, messageChan, dfunc := startMockSlackAndCreateSlack()
		testMessage := "test message"

		defer dfunc()

		s.SendMessage(Info, testMessage)
		checkMessage(t, BoxPrefixInfo+testMessage, messageChan)

		s.SendMessage(Warn, testMessage)
		checkMessage(t, BoxPrefixWarn+testMessage, messageChan)

		s.SendMessage(Error, testMessage)
		checkMessage(t, BoxPrefixError+testMessage, messageChan)

		s.SendMessage(Success, testMessage)
		checkMessage(t, BoxPrefixSuccess+testMessage, messageChan)
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

func checkMessage(t *testing.T, expectedMsg string, messageChan chan *slackGo.MessageEvent) {
	t.Helper()

	var (
		got *slackGo.MessageEvent
		ok  bool
	)

	testutil.Eventually(t, testMaxWait, 10*time.Millisecond, func() bool {
		select {
		case got = <-messageChan:
			ok = true

			return true
		default:
			return false
		}
	}, "slack message")

	if !ok {
		So(false, ShouldBeTrue, "did not get channel message in time")

		return
	}

	So(got.Channel, ShouldEqual, testChannel)
	So(got.Text, ShouldEqual, expectedMsg)
}

func checkNoMessage(t *testing.T, messageChan chan *slackGo.MessageEvent) bool {
	t.Helper()

	clean := true

	testutil.RequireStable(t, testSmallerMaxWait, 10*time.Millisecond, func() bool {
		select {
		case <-messageChan:
			clean = false

			return false
		default:
			return true
		}
	}, "no slack message")

	return clean
}
