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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const testTLSTimeout = 2 * time.Second

func TestStartTLSServer(t *testing.T) {
	Convey("Given a self-signed certificate for localhost and a free address", t, func() {
		certFile, keyFile := writeLocalhostCert(t)
		addr := freeLocalhostAddr(t)
		start, closeServers := certServerStarter(certFile, keyFile)

		Reset(closeServers)

		Convey("StartTLSServer starts a server there that the wait accepts", func() {
			got, errCh, err := StartTLSServer(start, addr, certFile, nil, testTLSTimeout)
			So(err, ShouldBeNil)
			So(got, ShouldEqual, addr)

			closeServers()

			select {
			case errs := <-errCh:
				So(errs, ShouldEqual, http.ErrServerClosed)
			case <-time.After(testTLSTimeout):
				So("server stop not reported", ShouldBeEmpty)
			}
		})

		Convey("When a server with a different certificate already has the port", func() {
			otherCert, otherKey := writeLocalhostCert(t)
			stopImpostor := serveTLSOn(t, addr, otherCert, otherKey)

			Reset(stopImpostor)

			Convey("WaitForTLSServer does not accept the other server", func() {
				err := WaitForTLSServer(addr, certFile, nil, 300*time.Millisecond)
				So(errors.Is(err, ErrTLSServerNotReady), ShouldBeTrue)
				So(err.Error(), ShouldContainSubstring, "certificate")
			})

			Convey("StartTLSServer moves to a new address and starts there", func() {
				got, _, err := StartTLSServer(start, addr, certFile, func() (string, error) {
					return freeLocalhostAddr(t), nil
				}, testTLSTimeout)
				So(err, ShouldBeNil)
				So(got, ShouldNotEqual, addr)
				So(WaitForTLSServer(got, certFile, nil, testTLSTimeout), ShouldBeNil)
			})

			Convey("StartTLSServer without newAddr fails clearly", func() {
				got, errCh, err := StartTLSServer(start, addr, certFile, nil, testTLSTimeout)
				So(got, ShouldEqual, addr)
				So(errors.Is(err, ErrTLSServerStartFailed), ShouldBeTrue)
				So(errors.Is(err, syscall.EADDRINUSE), ShouldBeTrue)
				So(errors.Is(<-errCh, syscall.EADDRINUSE), ShouldBeTrue)
			})
		})
	})
}

func writeLocalhostCert(t *testing.T) (string, string) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	So(err, ShouldBeNil)

	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	So(err, ShouldBeNil)

	keyDER, err := x509.MarshalECPrivateKey(priv)
	So(err, ShouldBeNil)

	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")

	So(os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600), ShouldBeNil)
	So(os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0600), ShouldBeNil)

	return certFile, keyFile
}

func freeLocalhostAddr(t *testing.T) string {
	t.Helper()

	ln, err := new(net.ListenConfig).Listen(context.Background(), "tcp", "localhost:0")
	So(err, ShouldBeNil)

	_, port, err := net.SplitHostPort(ln.Addr().String())
	So(err, ShouldBeNil)
	So(ln.Close(), ShouldBeNil)

	return net.JoinHostPort("localhost", port)
}

// certServerStarter returns a start function for StartTLSServer that serves
// the given certificate, and a function that closes every server it started.
func certServerStarter(certFile, keyFile string) (func(string) error, func()) {
	var (
		mu      sync.Mutex
		servers []*http.Server
	)

	start := func(addr string) error {
		srv := &http.Server{Addr: addr, Handler: http.NotFoundHandler(), ReadHeaderTimeout: time.Second}

		mu.Lock()

		servers = append(servers, srv)

		mu.Unlock()

		return srv.ListenAndServeTLS(certFile, keyFile)
	}

	closeAll := func() {
		mu.Lock()
		defer mu.Unlock()

		for _, srv := range servers {
			_ = srv.Close()
		}
	}

	return start, closeAll
}

// serveTLSOn serves the given certificate on addr, returning a function that
// stops the server.
func serveTLSOn(t *testing.T, addr, certFile, keyFile string) func() {
	t.Helper()

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	So(err, ShouldBeNil)

	ln, err := tls.Listen("tcp", addr, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	So(err, ShouldBeNil)

	srv := &http.Server{
		Handler:           http.NotFoundHandler(),
		ReadHeaderTimeout: time.Second,
		ErrorLog:          log.New(io.Discard, "", 0),
	}

	serveErr := make(chan error, 1)

	go func() { serveErr <- srv.Serve(ln) }()

	return func() {
		So(srv.Close(), ShouldBeNil)
		So(<-serveErr, ShouldEqual, http.ErrServerClosed)
	}
}
