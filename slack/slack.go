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
	"io"

	slackGo "github.com/slack-go/slack"
	gas "github.com/wtsi-hgi/go-authserver"
)

const (
	BoxPrefixInfo    = "⬜️ "
	BoxPrefixWarn    = "🟧 "
	BoxPrefixError   = "🟥 "
	BoxPrefixSuccess = "🟩 "
)

type Level int

const (
	Info Level = iota
	Warn
	Error
	Success
)

// Config provides configuration for a Slack.
type Config struct {
	// Token is the token you get from the OAuth&Permissions tab in your slack
	// application's features.
	Token string

	// Channel is the channel ID you get after pressing the 'Get channel
	// details' button (channel title) in any channel, the Channel ID is at the
	// bottom of the pop-up box.
	Channel string

	// URL is optional and only needs to be set when testing with a local mock
	// slack server.
	URL string

	// ErrorLogger is an optional place that any failures to send slack messages
	// are written to; to prevent issues our SendMessage() never returns an
	// error because it runs in a goroutine and returns immediately.
	ErrorLogger io.Writer
}

// Slack is something that lets you send messages to Slack.
type Slack struct {
	api     *slackGo.Client
	channel string
	logger  io.Writer
}

// New creates a new Slack using the Token, Channel and URL (if provided) from
// the Config.
//
// To get the token you must first create a Slack application, which needs to be
// a bot with these scopes added: chat:write, chat:write.customize,
// chat:write.public, groups:read and incoming-webhook, and then add this bot to
// your workspace.
func New(config Config) *Slack {
	var options []slackGo.Option
	if config.URL != "" {
		options = append(options, slackGo.OptionAPIURL(config.URL))
	}

	return &Slack{
		api:     slackGo.New(config.Token, options...),
		channel: config.Channel,
		logger:  config.ErrorLogger,
	}
}

// SendMessage sends the given message to our configured channel, prefixing it
// with a colour corresponding to its level.
//
// NB: this returns immediately, sending in a goroutine. To see errors, configer
// the slacker with an ErrorLogger.
func (s *Slack) SendMessage(level Level, msg string) {
	go func() {
		_, _, _, err := s.api.SendMessage(s.channel, slackGo.MsgOptionText(levelToPrefix(level)+msg, false))
		if s.logger != nil && err != nil {
			s.logger.Write([]byte(err.Error())) //nolint:errcheck
		}
	}()
}

func levelToPrefix(level Level) string {
	switch level {
	case Info:
		return BoxPrefixInfo
	case Warn:
		return BoxPrefixWarn
	case Error:
		return BoxPrefixError
	case Success:
		return BoxPrefixSuccess
	}

	return ""
}

type Mock struct {
	logger *gas.StringLogger
}

func NewMock(logger *gas.StringLogger) *Mock {
	return &Mock{logger: logger}
}

func (s *Mock) SendMessage(level Level, msg string) {
	s.logger.Write([]byte(levelToPrefix(level) + msg)) //nolint:errcheck
}
