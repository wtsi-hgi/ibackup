/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Authors:
 *	- Sendu Bala <sb10@sanger.ac.uk>
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

package cmd

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"syscall"

	server "github.com/wtsi-hgi/go-authserver"
	"golang.org/x/term"
)

const (
	privatePerms os.FileMode = 0600
	jwtBasename              = ".ibackup.jwt"
)

// JWTPermissionsError is used to distinguish this type of error - where the
// already stored JWT token doesn't have private permissions.
type JWTPermissionsError struct {
	tokenFile string
}

// Error is the print out string for JWTPermissionsError, so the user can
// see and rectify the permissions issue.
func (e JWTPermissionsError) Error() string {
	return fmt.Sprintf("Token %s does not have %v permissions "+
		"- won't use it", e.tokenFile, privatePerms)
}

// getJWT checks if we have stored a jwt in a file in user's home directory.
// If so, the JWT is refreshed and returned.
//
// Otherwise, we ask the user for the password and login, storing and returning
// the new JWT.
func getJWT(url, cert string) (string, error) {
	token, err := getStoredJWT(url, cert)
	if err == nil {
		return token, nil
	}

	if errors.As(err, &JWTPermissionsError{}) {
		return "", err
	}

	token, err = login(url, cert)
	if err == nil {
		err = storeJWT(token)
	}

	return token, err
}

// getStoredJWT sees if we've previously called storeJWT(), gets the token from
// the file it made, then tries to refresh it on the Server.
//
// We also check if the token has private permissions, otherwise we won't use
// it. This is as an attempt to reduce the likelihood of the token being leaked
// with its long expiry time (used so the user doesn't have to continuously log
// in, as we're not working with specific refresh tokens to get new access
// tokens).
func getStoredJWT(url, cert string) (string, error) {
	path, err := jwtStoragePath()
	if err != nil {
		return "", err
	}

	stat, err := os.Stat(path)
	if err != nil {
		return "", err
	}

	if stat.Mode() != privatePerms {
		return "", JWTPermissionsError{tokenFile: path}
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	token := strings.TrimSpace(string(content))

	return server.RefreshJWT(url, cert, token)
}

// jwtStoragePath returns the path where we store our JWT.
func jwtStoragePath() (string, error) {
	dir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, jwtBasename), nil
}

// login prompts the user for a password to log them in. Returns the JWT for the
// user.
func login(url, cert string) (string, error) {
	cliPrint("Password: ")

	passwordB, err := term.ReadPassword(syscall.Stdin)
	if err != nil {
		die("couldn't read password: %s", err)
	}

	cliPrint("\n")

	return server.Login(url, cert, currentUsername(), string(passwordB))
}

func currentUsername() string {
	user, err := user.Current()
	if err != nil {
		die("couldn't get user: %s", err)
	}

	return user.Username
}

// storeJWT writes the given token string to a private file in user's home dir.
func storeJWT(token string) error {
	path, err := jwtStoragePath()
	if err != nil {
		return err
	}

	return os.WriteFile(path, []byte(token), privatePerms)
}
