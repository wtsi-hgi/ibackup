/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
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

package testutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"time"
)

const (
	tlsDialTimeout   = 50 * time.Millisecond
	tlsPollInterval  = 10 * time.Millisecond
	maxStartAttempts = 3
)

var (
	// ErrTLSServerNotReady is returned when no server presenting the expected
	// certificate answered in time.
	ErrTLSServerNotReady = errors.New("no server presenting the expected certificate answered")

	// ErrTLSServerStartFailed is returned when a server's start function
	// returned before the server answered.
	ErrTLSServerStartFailed = errors.New("server failed to start")

	errBadCertFile = errors.New("no certificate found in file")
)

// WaitForTLSServer waits up to timeout for a server at addr that presents the
// self-signed certificate in certFile. A server presenting any other
// certificate, such as another process's test server listening on the same
// port, does not satisfy the wait.
//
// If startErr receives a value first, meaning the server's start function
// returned, WaitForTLSServer stops waiting and returns an
// ErrTLSServerStartFailed wrapping that value. startErr may be nil.
func WaitForTLSServer(addr, certFile string, startErr <-chan error, timeout time.Duration) error {
	pool, err := certPoolFromFile(certFile)
	if err != nil {
		return err
	}

	returned, err := awaitTLSServer(addr, pool, startErr, timeout)
	if returned {
		return startFailure(err)
	}

	return err
}

// StartTLSServer calls start(addr) in the background, then waits for the server
// as WaitForTLSServer does. If start fails because addr is already in use (eg.
// another process bound the port after it was chosen) and newAddr is not nil,
// it tries again on the address newAddr returns, up to 3 attempts in all.
//
// It returns the address the server answered on, and a channel that receives
// start's return value once the server stops. On failure it returns the error
// with the last attempt's address and channel; if that attempt's start had
// already returned, its value is put back in the channel.
func StartTLSServer(start func(addr string) error, addr, certFile string,
	newAddr func() (string, error), timeout time.Duration) (string, <-chan error, error) {
	pool, err := certPoolFromFile(certFile)
	if err != nil {
		return addr, nil, err
	}

	for attempt := 1; ; attempt++ {
		errCh := make(chan error, 1)
		attemptAddr := addr

		go func() { errCh <- start(attemptAddr) }()

		returned, err := awaitTLSServer(addr, pool, errCh, timeout)
		if !returned {
			return addr, errCh, err
		}

		errCh <- err

		if !shouldRetryStart(err, newAddr, attempt) {
			return addr, errCh, startFailure(err)
		}

		next, errn := newAddr()
		if errn != nil {
			return addr, errCh, errors.Join(startFailure(err), errn)
		}

		addr = next
	}
}

func certPoolFromFile(certFile string) (*x509.CertPool, error) {
	pemBytes, err := os.ReadFile(certFile)
	if err != nil {
		return nil, err
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("%w: %s", errBadCertFile, certFile)
	}

	return pool, nil
}

// awaitTLSServer polls addr until a TLS handshake verified against pool
// succeeds, startErr receives a value, or timeout passes. returned is true
// when startErr received a value, in which case err is that value.
func awaitTLSServer(addr string, pool *x509.CertPool, startErr <-chan error,
	timeout time.Duration) (returned bool, err error) {
	deadline := time.Now().Add(timeout)
	lastErr := ErrTLSServerNotReady

	for time.Now().Before(deadline) {
		select {
		case err = <-startErr:
			return true, err
		default:
		}

		if lastErr = dialTLS(addr, pool); lastErr == nil {
			return false, nil
		}

		time.Sleep(tlsPollInterval)
	}

	return false, fmt.Errorf("%w at %s: %w", ErrTLSServerNotReady, addr, lastErr)
}

func dialTLS(addr string, pool *x509.CertPool) error {
	ctx, cancel := context.WithTimeout(context.Background(), tlsDialTimeout)
	defer cancel()

	dialer := tls.Dialer{
		NetDialer: &net.Dialer{Timeout: tlsDialTimeout},
		Config:    &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
	}

	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}

	return conn.Close()
}

func shouldRetryStart(err error, newAddr func() (string, error), attempt int) bool {
	return newAddr != nil && attempt < maxStartAttempts && errors.Is(err, syscall.EADDRINUSE)
}

func startFailure(err error) error {
	return fmt.Errorf("%w: %w", ErrTLSServerStartFailed, err)
}
